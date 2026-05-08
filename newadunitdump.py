"""
Adunitdump.py  ·  GAM Sync + Enrichment + Email
================================================
What this flow does, in order:
  1. fetch_gam_data         – Pull active ad units from OLD GAM + New GAM
  2. read_master_sheet      – Read the current master GSheet into memory
                              (used as a lookup for the enricher)
  3. enrich_units           – For every new unit, derive:
                                Expresso Website Name, Business, Site, Platform,
                                Ad_Type, Ad_Position, Ad_Position_Granular,
                                Innovation, Section Names
                              Priority: exact match in master → rule-based logic
  4. sync_to_master_by_id   – Append new (enriched) rows to master GSheet
  5. find_direct_order_gaps – Check which GAM units are missing from the
                              Ad_Unit_Mapping tab (read-only)
  6. send_combined_email    – Email report with:
                                · CSV of newly added units (with all enriched cols)
                                · CSV of direct-order gaps (with suggested metadata)
"""

import os, sys, re, tempfile, subprocess, json, smtplib, io, csv
from datetime import datetime
from typing import Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 ── BOOTSTRAP
# ═══════════════════════════════════════════════════════════════════════════════

def install_dependencies():
    try:
        import gspread
        from googleads import ad_manager
        import pandas as pd
    except ImportError:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install",
            "gspread", "googleads", "pandas"
        ])


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 ── ENRICHMENT RULES
# (Self-contained, no external imports needed beyond re)
# ═══════════════════════════════════════════════════════════════════════════════

# Columns written to the GSheet after the base 10 dump columns
ENRICHED_COLS = [
    "Expresso Website Name",
    "Business",
    "Site",
    "Platform",
    "Ad_Type",
    "Ad_Position",
    "Ad_Position_Granular",
    "Innovation",
    "Section Names",
]

# ── Platform is the top-level discriminator for ATF/BTF/BO ad type ──────────
_WEB_PLATFORMS = {"Web"}          # Leaderboard family
_MOBILE_PLATFORMS = {"Mweb", "AMP", "IOS", "AOS", "CTV", "FireOS", "OTT"}

# ── Suffix → (Ad_Type, Ad_Position, Ad_Position_Granular, Innovation) ────────
# For ATF / BTF / BO the tuple contains a SENTINEL "__PLATFORM__" so the
# resolver can pick Web vs Mobile values at runtime.
_WEB   = "__WEB__"
_MOB   = "__MOB__"

_PLATFORM_SPLIT: dict[str, tuple] = {
    # suffix: (web_tuple, mobile_tuple)
    "ATF": (
        ("Leaderboard",    "ATF", "ATF", "TIL_Leaderboard"),
        ("Top Banner",     "ATF", "ATF", "TIL_Top Banner, Page Push Down, WAP Page Push Down"),
    ),
    "BTF": (
        ("Leaderboard",    "BTF", "BTF", "TIL_Leaderboard"),
        ("Bottom Overlay", "ATF", "ATF", "TIL_Bottom Overlay"),
    ),
    "BO": (
        ("Leaderboard",    "BTF", "BTF", "TIL_Leaderboard"),
        ("Bottom Overlay", "ATF", "ATF", "TIL_Bottom Overlay"),
    ),
}

# Variants of ATF / BTF that carry the same platform-split logic
_ATF_VARIANTS = {
    "ATF_P1", "ATF_P2", "ATF_M1", "ATF_M1_P1", "ATF_M2", "ATF_M2_P1",
    "ATF_M3", "ATF_M3_P1", "ATF_R1", "ATF_R2", "ATF_R3", "ATF_REF",
}
_BTF_VARIANTS = {
    "BTF_P1", "BTF_P2", "BTF_M1", "BTF_M2", "BTF_M3",
    "BTF_R1", "BTF_R2", "BTF_R3", "BTF_INF",
    "BTF_1", "BTF_2", "BTF_3", "BTF_4", "BTF_5",
    "BO_M1", "BO_M2", "BO_M3", "BO_P1", "BO_R1", "BO_R2", "BO_R3",
    "BO_REF", "BO_CR1", "BO_CR2", "BO_CR_1", "BO_CR_2",
}
# BTF_REF: Mobile = Bottom Overlay (confirmed), Web = Leaderboard
_BO_VARIANTS = {"BTF_REF"}

_SUFFIX_MAP: dict[str, tuple] = {
    # ── Leaderboard-only (always, regardless of platform) ───────────────────
    "ATF_1":        ("Leaderboard",    "ATF",         "ATF",          ""),
    "ATF_2":        ("Leaderboard",    "ATF",         "ATF",          ""),
    "ATF_A1":       ("Leaderboard",    "ATF",         "ATF",          "TIL_Leaderboard,TIL_Billboard"),
    "ATF_A2":       ("Leaderboard",    "ATF",         "ATF",          "TIL_Leaderboard,TIL_Billboard"),
    "ATF_PPD":      ("Leaderboard",    "ATF",         "ATF",          "TIL_Feature Banner"),
    "AFF_ATF":      ("Leaderboard",    "ATF",         "ATF",          "TIL_Leaderboard"),
    "SW":           ("Leaderboard",    "ATF",         "ATF",          "TIL_Leaderboard"),
    "LB":           ("Leaderboard",    "ATF",         "ATF",          "TIL_Leaderboard"),
    "BTF_INF":      ("Leaderboard",    "BTF",         "BTF",          "TIL_Leaderboard"),

    # ── Bottom Overlay-only ──────────────────────────────────────────────────
    "AFF_BO":       ("Bottom Overlay", "ATF",         "ATF",          "TIL_Bottom Overlay"),

    # ── Interstitial ─────────────────────────────────────────────────────────
    "INT":          ("Interstitial",   "Out Of Page", "Out Of Page",  "TIL_Interstitial"),
    "INT_P1":       ("Interstitial",   "Out Of Page", "Out Of Page",  "TIL_Interstitial"),
    "INT_PRE":      ("Interstitial",   "Out Of Page", "Out Of Page",  "TIL_Interstitial"),
    "INT_1":        ("Interstitial",   "Out Of Page", "Out Of Page",  "Google Viggnete"),
    "INT_2":        ("Interstitial",   "Out Of Page", "Out Of Page",  "Google Viggnete"),
    "INT_3":        ("Interstitial",   "Out Of Page", "Out Of Page",  "Google Viggnete"),
    "INT_4":        ("Interstitial",   "Out Of Page", "Out Of Page",  "Google Viggnete"),
    "INT_5":        ("Interstitial",   "Out Of Page", "Out Of Page",  "TIL_Interstitial"),
    "GOOGLE_INTERSTITIAL": ("Interstitial", "Out Of Page", "Out Of Page", "TIL_Interstitial"),

    # ── Skinner / Innovation ─────────────────────────────────────────────────
    "INNOV":        ("Skinner",        "Out Of Page", "Out Of Page",  "TIL_Skin"),
    "INNOV_M1":     ("Skinner",        "Out Of Page", "Out Of Page",  "TIL_Skin"),
    "INNOV_M2":     ("Skinner",        "Out Of Page", "Out Of Page",  "TIL_Skin"),
    "INNOV_M3":     ("Skinner",        "Out Of Page", "Out Of Page",  "TIL_Skin"),
    "INNOV_A1":     ("Skinner",        "Out Of Page", "Out Of Page",  "TIL_Skin"),
    "INNOV_A2":     ("Skinner",        "Out Of Page", "Out Of Page",  "TIL_Skin"),
    "AFF_INNOV":    ("Skinner",        "Out Of Page", "Out Of Page",  "Skin,TIL_Skin"),
    "SKINNER_LHS":  ("Skinner",        "LHS",         "LHS",          ""),
    "SKINNER_RHS":  ("Skinner",        "RHS",         "RHS",          ""),

    # ── Earpanel ─────────────────────────────────────────────────────────────
    "EARPANEL_LHS": ("Earpanel",       "LHS",         "LHS",          "TIL_Ear Panel LHS"),
    "EARPANEL_RHS": ("Earpanel",       "RHS",         "RHS",          "TIL_Ear Panel RHS"),

    # ── Flying Carpet (all BTF — confirmed) ──────────────────────────────────
    "FC":           ("Flying Carpet",  "BTF",         "BTF",          "TIL_Flying Carpet"),
    "FC_P1":        ("Flying Carpet",  "BTF",         "BTF",          "TIL_Flying Carpet"),
    "FC_P2":        ("Flying Carpet",  "BTF",         "BTF",          "TIL_Flying Carpet"),
    "FC_REF":       ("Flying Carpet",  "BTF",         "BTF",          "TIL_Flying Carpet"),

    # ── Top Band ─────────────────────────────────────────────────────────────
    "TOPBAND":      ("Top Band",       "ATF",         "ATF",          "TIL_Top Band"),

    # ── Beacon ───────────────────────────────────────────────────────────────
    "BEACON":       ("Beacon",         "ATF",         "ATF",          "TIL_Beacon"),
    "BEACON_1X1":   ("Beacon",         "ATF",         "ATF",          "TIL_Beacon"),

    # ── Cube ─────────────────────────────────────────────────────────────────
    "CUBELOGO_1":   ("Cube",           "ATF",         "ATF",          "TIL_Cube"),
    "CUBELOGO_2":   ("Cube",           "ATF",         "ATF",          "TIL_Cube"),
    "CUBETEXT_1":   ("Cube",           "ATF",         "ATF",          "TIL_Cube"),
    "CUBETEXT_2":   ("Cube",           "ATF",         "ATF",          "TIL_Cube"),

    # ── Story Ad ─────────────────────────────────────────────────────────────
    "STORYAD":      ("Auto Ads",       "ATF",         "ATF",          "TIL_Interstitial"),
    "STORYAD_M1":   ("Auto Ads",       "ATF",         "ATF",          "TIL_Interstitial"),
    "STORYAD_M2":   ("Auto Ads",       "ATF",         "ATF",          "TIL_Interstitial"),
    "STORYAD_M3":   ("Auto Ads",       "ATF",         "ATF",          "TIL_Interstitial"),

    # ── L-Band ───────────────────────────────────────────────────────────────
    "LBAND":        ("LBand",          "ATF",         "ATF",          "L-Band"),
    "MINITV_LBAND": ("Lband",          "ATF",         "ATF",          "L-Band"),

    # ── Slug ─────────────────────────────────────────────────────────────────
    **{f"SLUG_{i}": ("Slug", "BTF", "BTF", "TIL_Slug") for i in range(1, 11)},
    "SLUGFLIP_1":   ("Slug",           "BTF",         "BTF",          "TIL_Slug"),
    "SLUGFLIP_2":   ("Slug",           "BTF",         "BTF",          "TIL_Slug"),

    # ── MTF family ───────────────────────────────────────────────────────────
    **{f"MTF_{i}": ("Mrec", "MTF", "MTF Mrec", "Mrec,TIL_Mrec,TIL_In-banner Video") for i in range(1, 6)},
    "MTF_INF":      ("Mrec",           "MTF",         "INF Mrec",     "Mrec,TIL_Mrec,TIL_In-banner Video"),

    # ── Video ─────────────────────────────────────────────────────────────────
    "PREROLL":      ("Video",          "Preroll",     "Preroll",      "TIL_Instream Pre-Roll User Initiated"),
    "POSTROLL":     ("Video",          "Postroll",    "ATF",          "TIL_Instream Pre-Roll User Initiated"),
    "MIDROLL":      ("Video",          "Midroll",     "1st Midroll",  "TIL_Instream Pre-Roll User Initiated"),
}

_ORDINAL = {
    1: "ATF", 2: "2nd Mrec", 3: "3rd Mrec", 4: "4th Mrec",
    5: "5th Mrec", 6: "6th Mrec", 7: "7th Mrec", 8: "8th Mrec",
    9: "9th Mrec", 10: "10th Mrec",
}
_MIDORD = {1: "1st Midroll", 2: "2nd Midroll", 3: "3rd Midroll"}

_MREC_RE   = re.compile(r"^(AFF_)?MREC_(\d+|INF)(_(P[12]|R[123]|M[123]|A[12]|CR[12]|REF|HD))*$", re.I)
_VIDEO_RE  = re.compile(r"^(.+?)_(PREROLL|POSTROLL|MIDROLL_?(\d*))$", re.I)
_MTF_RE    = re.compile(r"^MTF_(\d+|INF)$", re.I)


def _resolve_suffix(suffix: str, platform: str) -> Optional[tuple]:
    """
    Return (Ad_Type, Ad_Position, Ad_Position_Granular, Innovation) or None.
    Platform is used to disambiguate ATF/BTF/BO families.
    """
    upper = suffix.upper()
    is_web = platform in _WEB_PLATFORMS

    # Platform-split ATF / BTF / BO base tokens
    if upper in _PLATFORM_SPLIT:
        web_t, mob_t = _PLATFORM_SPLIT[upper]
        return web_t if is_web else mob_t

    # ATF variants → same split as ATF
    if upper in {v.upper() for v in _ATF_VARIANTS}:
        web_t, mob_t = _PLATFORM_SPLIT["ATF"]
        return web_t if is_web else mob_t

    # BTF variants → same split as BTF
    if upper in {v.upper() for v in _BTF_VARIANTS}:
        web_t, mob_t = _PLATFORM_SPLIT["BTF"]
        return web_t if is_web else mob_t

    # BTF_REF: mobile = Bottom Overlay, web = Leaderboard (confirmed anomaly)
    if upper == "BTF_REF":
        if is_web:
            return ("Leaderboard", "BTF", "BTF", "TIL_Leaderboard")
        return ("Bottom Overlay", "ATF", "ATF", "TIL_Bottom Overlay")

    # Direct lookup (case-insensitive)
    for k, v in _SUFFIX_MAP.items():
        if k.upper() == upper:
            return v

    # MREC_N family
    if _MREC_RE.match(upper):
        core = re.sub(r"^AFF_", "", upper)
        num_part = core.split("_")[1]
        if num_part == "INF":
            gran, pos = "INF Mrec", "BTF"
        else:
            n = int(num_part)
            gran = _ORDINAL.get(n, f"{n}th Mrec") if n > 1 else "ATF"
            pos  = "ATF" if n == 1 else "BTF"
        return ("Mrec", pos, gran, "Mrec,TIL_Mrec,TIL_In-banner Video")

    # MTF_N family
    if _MTF_RE.match(upper):
        return ("Mrec", "MTF", "MTF Mrec", "Mrec,TIL_Mrec,TIL_In-banner Video")

    # Video families: *_PREROLL, *_POSTROLL, *_MIDROLL_N
    m = _VIDEO_RE.match(upper)
    if m:
        action = m.group(2).upper()
        if "PREROLL" in action:
            return ("Video", "Preroll", "Preroll", "TIL_Instream Pre-Roll User Initiated")
        if "POSTROLL" in action:
            return ("Video", "Postroll", "ATF", "TIL_Instream Pre-Roll User Initiated")
        if "MIDROLL" in action:
            n = int(m.group(3)) if m.group(3) else 1
            gran = _MIDORD.get(n, f"{n}th Midroll")
            return ("Video", "Midroll", gran, "TIL_Instream Pre-Roll User Initiated")

    return None


# ── Ad unit 1 → (Platform, Business, Site, Expresso Website Name) ────────────
_AD_UNIT_1_MAP: dict[str, tuple] = {
    "TOI_MWEB":        ("Mweb",   "TOI",       "TOI",              "TOI Mobile Site"),
    "TOI_WEB":         ("Web",    "TOI",       "TOI",              "TOI Website"),
    "TOI_AMP":         ("AMP",    "TOI",       "TOI",              "TOI AMP"),
    "TOI_IOS":         ("IOS",    "TOI",       "TOI",              "TOI IOS Apps"),
    "TOI_AOS":         ("AOS",    "TOI",       "TOI",              "TOI Android Apps"),
    "ETIMES_AMP":      ("AMP",    "TOI",       "Etimes",           "E-TIMES AMP"),
    "ETIMES_WEB":      ("Web",    "TOI",       "Etimes",           "E-TIMES WEBSITE"),
    "ETIMES_MWEB":     ("Mweb",   "TOI",       "Etimes",           "E-TIMES MWEB"),
    "ETIMES_IOS":      ("IOS",    "TOI",       "Etimes",           "E-TIMES IOS"),
    "ETIMES_AOS":      ("AOS",    "TOI",       "Etimes",           "E-TIMES AOS"),
    "ET_AMP":          ("AMP",    "ET",        "ET",               "ET AMP MWEB"),
    "ET_WEB":          ("Web",    "ET",        "ET",               "ET Website"),
    "ET_MWEB":         ("Mweb",   "ET",        "ET",               "ET Mobile Site"),
    "ET_AOS":          ("AOS",    "ET",        "ET",               "ET Android Apps"),
    "ET_IOS":          ("IOS",    "ET",        "ET",               "ET IOS Apps"),
    "ETMARKETS_AOS":   ("AOS",    "ET",        "ETMarket",         "ETMKT Android App"),
    "ETMARKETS_IOS":   ("IOS",    "ET",        "ETMarket",         "ETMKT IOS Apps"),
    "NBT_AMP":         ("AMP",    "Languages", "NBT",              "NBT AMP website"),
    "NBT_WEB":         ("Web",    "Languages", "NBT",              "NBT Website"),
    "NBT_MWEB":        ("Mweb",   "Languages", "NBT",              "NBT Mobile Site"),
    "NBT_AOS":         ("AOS",    "Languages", "NBT",              "NBT Android Apps"),
    "NBT_IOS":         ("IOS",    "Languages", "NBT",              "NBT IOS Apps"),
    "MT_MWEB":         ("Mweb",   "Languages", "MT",               "MT Mobile Site"),
    "MT_AMP":          ("AMP",    "Languages", "MT",               "MT Amp site"),
    "MT_WEB":          ("Web",    "Languages", "MT",               "MT Website"),
    "MT_IOS":          ("IOS",    "Languages", "MT",               "MT IOS Apps"),
    "MT_AOS":          ("AOS",    "Languages", "MT",               "MT Android Apps"),
    "VK_AMP":          ("AMP",    "Languages", "VK",               "VijayKarnataka Amp Site"),
    "VK_MWEB":         ("Mweb",   "Languages", "VK",               "VijayKarnataka Mobile Website"),
    "VK_WEB":          ("Web",    "Languages", "VK",               "VijayKarnataka Website"),
    "VK_IOS":          ("IOS",    "Languages", "VK",               "VijayKarnataka IOS Apps"),
    "VK_AOS":          ("AOS",    "Languages", "VK",               "VijayKarnataka Android Apps"),
    "TLG_AMP":         ("AMP",    "Languages", "Telugu",           "Telgu Samayam Amp site"),
    "TLG_MWEB":        ("Mweb",   "Languages", "Telugu",           "Telgu Samayam Mobile Site"),
    "TLG_WEB":         ("Web",    "Languages", "Telugu",           "Telugu Samayam website"),
    "TLG_AOS":         ("AOS",    "Languages", "Telugu",           "Telgu Samayam Android APP"),
    "TLG_IOS":         ("IOS",    "Languages", "Telugu",           "Telugu samayam iOS APP"),
    "TML_WEB":         ("Web",    "Languages", "Tamil",            "Tamil Samayam website"),
    "TML_AMP":         ("AMP",    "Languages", "Tamil",            "Tamil Amp site"),
    "TML_MWEB":        ("Mweb",   "Languages", "Tamil",            "Tamil Samayam Mobile Website"),
    "TML_AOS":         ("AOS",    "Languages", "Tamil",            "TAMIL SAMAYAM ANDROID"),
    "TML_IOS":         ("IOS",    "Languages", "Tamil",            "TAMIL SAMAYAM iOS APP"),
    "MS_MWEB":         ("Mweb",   "Languages", "Malayalam",        "Malyalam Samayam Mobile Site"),
    "MS_AMP":          ("AMP",    "Languages", "Malayalam",        "Malyalam Samayam Amp site"),
    "MS_WEB":          ("Web",    "Languages", "Malayalam",        "Malyalam Samayan Website"),
    "MS_IOS":          ("IOS",    "Languages", "Malayalam",        "Malyalam Samayam IOS APP"),
    "MS_AOS":          ("AOS",    "Languages", "Malayalam",        "Malyalam Samayam Android"),
    "IAG_MWEB":        ("Mweb",   "Languages", "IAG",              "Iamgujarat mobile website"),
    "IAG_WEB":         ("Web",    "Languages", "IAG",              "Iamgujarat website"),
    "IAG_AMP":         ("AMP",    "Languages", "IAG",              "Iamgujarat AMP Website"),
    "IAG_AOS":         ("AOS",    "Languages", "IAG",              "Iamgujarat AOS"),
    "IAG_IOS":         ("IOS",    "Languages", "IAG",              "Iamgujarat IOS"),
    "ITBANGLA_WEB":    ("Web",    "Languages", "IT Bangla",        "ITBANGLA_WEB"),
    "ITBANGLA_MWEB":   ("Mweb",   "Languages", "IT Bangla",        "ITBANGLA_MWEB"),
    "ITBANGLA_AMP":    ("AMP",    "Languages", "IT Bangla",        "ITBANGLA_AMP"),
    "ETBENGALI_WEB":   ("Web",    "Languages", "ET Bengali",       "ET Bengali Website"),
    "ETBENGALI_MWEB":  ("Mweb",   "Languages", "ET Bengali",       "ET Bengali Mweb"),
    "ETBENGALI_AMP":   ("AMP",    "Languages", "ET Bengali",       "ET Bengali AMP"),
    "ETGUJARATI_AMP":  ("AMP",    "Languages", "ET Gujarati",      "ET Gujarati AMP"),
    "ETGUJARATI_WEB":  ("Web",    "Languages", "ET Gujarati",      "ET Gujarati Website"),
    "ETGUJARATI_MWEB": ("Mweb",   "Languages", "ET Gujarati",      "ET Gujarati Mweb"),
    "ETMARATHI_AMP":   ("AMP",    "Languages", "ET Marathi",       "ET Marathi AMP"),
    "ETMARATHI_WEB":   ("Web",    "Languages", "ET Marathi",       "ET Marathi Website"),
    "ETMARATHI_MWEB":  ("Mweb",   "Languages", "ET Marathi",       "ET Marathi Mweb"),
    "ETTAMIL_MWEB":    ("Mweb",   "Languages", "ET Tamil",         "ET Tamil Mweb"),
    "ETTAMIL_AMP":     ("AMP",    "Languages", "ET Tamil",         "ET Tamil AMP"),
    "ETTAMIL_WEB":     ("Web",    "Languages", "ET Tamil",         "ET Tamil Website"),
    "ETHINDI_WEB":     ("Web",    "Languages", "ET Hindi",         "ET HINDI WEBSITE"),
    "ETHINDI_AMP":     ("AMP",    "Languages", "ET Hindi",         "ET_Hindi_AMP"),
    "ETHINDI_MWEB":    ("Mweb",   "Languages", "ET Hindi",         "ET_Hindi_mweb"),
    "ETKANNADA_MWEB":  ("Mweb",   "Languages", "ET Kannada",       "ET Kannada Mweb"),
    "ETKANNADA_WEB":   ("Web",    "Languages", "ET Kannada",       "ET Kannada Website"),
    "ETKANNADA_AMP":   ("AMP",    "Languages", "ET Kannada",       "ET Kannada AMP"),
    "ETTELUGU_WEB":    ("Web",    "Languages", "ET Telugu",        "ET Telugu Website"),
    "ETTELUGU_MWEB":   ("Mweb",   "Languages", "ET Telugu",        "ET Telugu Mweb"),
    "ETTELUGU_AMP":    ("AMP",    "Languages", "ET Telugu",        "ET Telugu AMP"),
    "ETMALAYALAM_MWEB":("Mweb",   "Languages", "ET Malayalam",     "ET Malayalam Mweb"),
    "ETMALAYALAM_AMP": ("AMP",    "Languages", "ET Malayalam",     "ET Malayalam AMP"),
    "ETMALAYALAM_WEB": ("Web",    "Languages", "ET Malayalam",     "ET Malayalam Website"),
    "MYLIFEXP_WEB":    ("Web",    "Languages", "MyLifeXP",         "My LifeXp Website"),
    "MYLIFEXP_AMP":    ("AMP",    "Languages", "MyLifeXP",         "My LifeXp AMP"),
    "MYLIFEXP_MWEB":   ("Mweb",   "Languages", "MyLifeXP",         "My LifeXp Mobile Site"),
    "NEWSPOINT_MWEB":  ("Mweb",   "Newspoint", "Newspoint",        "Newspoint MWEB"),
    "NEWSPOINT_AOS":   ("AOS",    "Newspoint", "Newspoint",        "NewsPoint Android Apps"),
    "NEWSPOINT_AMP":   ("AMP",    "Newspoint", "Newspoint",        "Newspoint MWEB"),
    "NP_MWEB":         ("Mweb",   "Newspoint", "Newspoint",        "Newspoint MWEB"),
    "NP_AMP":          ("AMP",    "Newspoint", "Newspoint",        "NEWSPOINT_AMP"),
    "GN_WEB":          ("Web",    "TOI",       "GN",               "Gadgetsnow website"),
    "GN_MWEB":         ("Mweb",   "TOI",       "GN",               "Gadgetsnow mobile site"),
    "GN_AMP":          ("AMP",    "TOI",       "GN",               "Gadgetsnow AMP site"),
    "TIMESPETS_WEB":   ("Web",    "TOI",       "Timespets",        "Times Pets Website"),
    "TIMESPETS_MWEB":  ("Mweb",   "TOI",       "Timespets",        "Times Pets Mobile Website"),
    "TIMESPETS_AMP":   ("AMP",    "TOI",       "Timespets",        "Times Pets AMP"),
    "TIMESLIFE_MWEB":  ("Mweb",   "TOI",       "TimesLife",        "Times Life Mobile Site"),
    "TIMESLIFE_WEB":   ("Web",    "TOI",       "TimesLife",        "Times Life Website"),
    "TIMESLIFE_AMP":   ("AMP",    "TOI",       "TimesLife",        "Times Life AMP"),
    "TOI_ADBLOCK":     ("Web",    "TOI",       "TOI Adblock",      "TOI_ADBLOCK"),
    "ETIMES_ADBLOCK":  ("Mweb",   "TOI",       "Etimes Adblock",   "ETIMES_ADBLOCK"),
    "TIL_TEST":        ("Web",    "Test",      "TOI",              "TIL_TEST"),
    "TIL_TRACKER":     ("Web",    "Tracker",   "TOI",              "TIL_TRACKER"),
    "WILLOWTV_WEB":    ("Web",    "Willow",    "Willow",           "Willow TV Website"),
    "WILLOWTV_CTV":    ("CTV",    "Willow",    "Willow",           "WILLOWTV CTV"),
    "WILLOWTV_AOS":    ("AOS",    "Willow",    "Willow",           "WillowTV APP ANDROID"),
    "WILLOWTV_IOS":    ("IOS",    "Willow",    "Willow",           "WillowTV APP iOS"),
    "WILLOWTV_MWEB":   ("Mweb",   "Willow",    "Willow",           "Willow TV Mobile site"),
    "WILLOWTV_FOS":    ("FireOS", "Willow",    "Willow",           "WILLOWTV_FOS"),
    "WILLOWTV_OTT":    ("OTT",    "Willow",    "Willow",           "WILLOWTV_OTT"),
    "CRICBUZZ_OTT":    ("OTT",    "Cricbuzz",  "Cricbuzz",         "CRICBUZZ_OTT"),
    "WBC_MWEB":        ("Mweb",   "Willow",    "WillowByCricbuzz", "WBC_MWEB"),
    "WBC_IOS":         ("IOS",    "Willow",    "WillowByCricbuzz", "WBC_IOS"),
    "WBC_AOS":         ("AOS",    "Willow",    "WillowByCricbuzz", "WBC_AOS"),
    "WBC_WEB":         ("Web",    "Willow",    "WillowByCricbuzz", "WBC_WEB"),
    "WBC_OTT":         ("OTT",    "Willow",    "WillowByCricbuzz", "WBC_OTT"),
    "ETB2B_WEB":       ("Web",    "ET B2B",    "ET B2B",           "ETB2B_WEB"),
    "ETB2B_IOS":       ("IOS",    "ET B2B",    "ET B2B",           "ETB2B_IOS"),
    "ETB2B_AOS":       ("AOS",    "ET B2B",    "ET B2B",           "ETB2B_AOS"),
    "ETB2B_AMP":       ("AMP",    "ET B2B",    "ET B2B",           "ETB2B_AMP"),
    "MM_MWEB":         ("Mweb",   "TOI",       "Mumbai Mirror",    "Mumbai Mirror Mobile Website"),
    "MM_WEB":          ("Web",    "TOI",       "Mumbai Mirror",    "MumbaiMirror Website"),
    "MM_AMP":          ("AMP",    "TOI",       "Mumbai Mirror",    "MumbaiMirror AMP website"),
    "BM_MWEB":         ("Mweb",   "TOI",       "Banglore Mirror",  "Bangalore Mirror Mobile Website"),
    "BM_WEB":          ("Web",    "TOI",       "Banglore Mirror",  "Bangalore Mirror Website"),
    "BM_AMP":          ("AMP",    "TOI",       "Banglore Mirror",  "Bangalore Mirror AMP Website"),
    "INDIATIMES_WEB":  ("Web",    "ILN",       "Indiatimes",       "IT Website"),
    "INDIATIMES_MWEB": ("Mweb",   "ILN",       "Indiatimes",       "INDIATIMES Mobile Site"),
    "INDIATIMES_AMP":  ("AMP",    "ILN",       "Indiatimes",       "Indiatimes_AMP_Mweb"),
    "ITIMES_WEB":      ("Web",    "TOI",       "Itimes",           "ITIMES_WEB"),
    "ITIMES_MWEB":     ("Mweb",   "TOI",       "Itimes",           "ITIMES_MWEB"),
    "TOIMARKETS_AOS":  ("AOS",    "TOI",       "TOIMarket",        "TOIMARKETS_AOS"),
    "TOIMARKETS_IOS":  ("IOS",    "TOI",       "TOIMarket",        "TOIMARKETS_IOS"),
}

_SPECIAL_SECTION: dict[str, str] = {
    "HOME":    "HP,Home",
    "US":      "HP,Home",
    "PSBK":    "PASSBACK",
    "IPL":     "IPL, CRICKET, SPORTS, RoS",
    "CRICKET": "CRICKET, SPORTS, RoS",
    "SPORTS":  "SPORTS, RoS",
}
_CAN_RE = re.compile(r"^(.+)_CAN$", re.I)
# Sorted once at import time for fast prefix matching
_AU1_KEYS_SORTED = sorted(_AD_UNIT_1_MAP.keys(), key=len, reverse=True)


def _resolve_au1(au1: str) -> Optional[tuple]:
    key_upper = au1.upper()
    for k in _AU1_KEYS_SORTED:
        if k.upper() == key_upper:
            return _AD_UNIT_1_MAP[k]
    # Colombia CAN units inherit brand metadata, override business → Colombia
    m = _CAN_RE.match(au1)
    if m:
        base = m.group(1)
        for k in _AU1_KEYS_SORTED:
            if k.upper() == base.upper():
                t = _AD_UNIT_1_MAP[k]
                return ("AMP", "Colombia", t[2], au1)
    return None


def _infer_section(au2: str) -> str:
    parts = str(au2).split("_")
    if len(parts) < 3:
        return ""
    topic = parts[2].upper()
    return _SPECIAL_SECTION.get(topic, f"{topic}, RoS")


def enrich_unit(unit: dict, master_lookup: dict) -> dict:
    """
    Enrich a single ad unit dict (as produced by fetch_gam_data).

    The unit dict has keys: Source, Ad unit 1, Ad unit 2, Ad unit 3,
    Final Ad unit, Ad unit 1 ID, Ad unit 2 ID, Ad unit 3 ID,
    Final Ad unit ID, Status.

    'Final Ad unit' is the full 4-level name (= Ad unit 4 in master).

    Returns the same dict with ENRICHED_COLS added.
    """
    ad4 = str(unit.get("Final Ad unit", "")).strip()
    enriched = {c: "" for c in ENRICHED_COLS}

    if not ad4:
        unit.update(enriched)
        return unit

    # ── Priority 1: exact match in master ────────────────────────────────────
    if ad4 in master_lookup:
        unit.update(master_lookup[ad4])
        return unit

    # ── Priority 2: rule-based ────────────────────────────────────────────────
    # Resolve Ad unit 1 from the full name (handles multi-segment prefixes)
    ad4_upper = ad4.upper()
    matched_au1 = None
    for key in _AU1_KEYS_SORTED:
        if ad4_upper.startswith(key.upper() + "_") or ad4_upper == key.upper():
            matched_au1 = key
            break

    # Platform / Business / Site / Expresso Website Name
    if matched_au1:
        au1_attrs = _resolve_au1(matched_au1)
        if au1_attrs:
            enriched["Platform"]              = au1_attrs[0]
            enriched["Business"]              = au1_attrs[1]
            enriched["Site"]                  = au1_attrs[2]
            enriched["Expresso Website Name"] = au1_attrs[3]

    platform = enriched["Platform"]

    # Section Names from Ad unit 2
    au2 = str(unit.get("Ad unit 2", "")).strip()
    if au2:
        enriched["Section Names"] = _infer_section(au2)

    # Suffix extraction: everything after Au1 + section + sub-section
    if matched_au1:
        au1_parts = len(matched_au1.split("_"))
        all_parts = ad4.split("_")
        # Au1 takes au1_parts segments; next 2 are section/sub; rest = suffix
        suffix_parts = all_parts[au1_parts + 2:]
        suffix = "_".join(suffix_parts)
    else:
        all_parts = ad4.split("_")
        suffix = "_".join(all_parts[4:]) if len(all_parts) > 4 else (all_parts[-1] if all_parts else "")

    if suffix:
        resolved = _resolve_suffix(suffix, platform)
        if resolved:
            enriched["Ad_Type"]              = resolved[0]
            enriched["Ad_Position"]          = resolved[1]
            enriched["Ad_Position_Granular"] = resolved[2]
            enriched["Innovation"]           = resolved[3]

    unit.update(enriched)
    return unit


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 ── PREFECT TASKS
# ═══════════════════════════════════════════════════════════════════════════════

@task(retries=2, retry_delay_seconds=60)
def fetch_gam_data(cfg):
    from googleads import ad_manager
    logger = get_run_logger()
    json_key = Secret.load("oldgamkey" if cfg['label'] == 'OLD GAM' else "newgamkey").get()
    if isinstance(json_key, dict):
        json_key = json.dumps(json_key)

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(json_key); tmp_path = tmp.name
    try:
        yaml = (f"ad_manager:\n  application_name: 'GAM_Dump'\n"
                f"  network_code: '{cfg['network_code']}'\n"
                f"  path_to_private_key_file: '{tmp_path}'")
        client = ad_manager.AdManagerClient.LoadFromString(yaml)
        service = client.GetService('InventoryService', version='v202602')
        all_units, offset = [], 0
        while True:
            q = (f"WHERE status = 'ACTIVE' ORDER BY id ASC LIMIT 1000 OFFSET {offset}"
                 if cfg['status_filter']
                 else f"ORDER BY id ASC LIMIT 1000 OFFSET {offset}")
            res = service.getAdUnitsByStatement({'query': q})
            if 'results' in res:
                all_units.extend(res['results']); offset += 1000
                if len(res['results']) < 1000: break
            else: break

        unit_map = {u.id: {'name': u.name, 'parentId': getattr(u, 'parentId', None)}
                    for u in all_units}
        processed = []
        for u in all_units:
            p_names, p_ids, curr = [], [], u.id
            while curr:
                info = unit_map.get(curr)
                if not info: break
                p_names.append(info['name']); p_ids.append(str(curr))
                curr = info['parentId']
            p_names.reverse(); p_ids.reverse()
            if len(p_ids) != cfg['depth']: continue
            if cfg['target_ids'] and not any(str(pid) in p_ids for pid in cfg['target_ids']): continue
            sn = p_names[cfg['skip_levels']:]
            si = p_ids[cfg['skip_levels']:]
            processed.append({
                'Source':           cfg['label'],
                'Ad unit 1':        sn[0] if sn else "",
                'Ad unit 2':        sn[1] if len(sn) > 1 else "",
                'Ad unit 3':        sn[2] if len(sn) > 2 else "",
                'Final Ad unit':    sn[-1] if sn else "",
                'Ad unit 1 ID':     si[0] if si else "",
                'Ad unit 2 ID':     si[1] if len(si) > 1 else "",
                'Ad unit 3 ID':     si[2] if len(si) > 2 else "",
                'Final Ad unit ID': si[-1] if si else "",
                'Status':           u.status,
            })
        logger.info(f"[{cfg['label']}] Fetched {len(processed)} ad units")
        return processed
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)


@task
def read_master_sheet(sheet_id: str) -> dict:
    """
    Read the master GSheet and return a lookup dict:
        { 'Final Ad unit name' : { enriched_col: value, ... } }
    Used by the enricher to do exact-match lookups before falling back to rules.
    """
    import gspread
    logger = get_run_logger()
    auth = Secret.load("newgamkey").get()
    if isinstance(auth, dict): auth = json.dumps(auth)
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(auth); tmp_path = tmp.name
    try:
        gc = gspread.service_account(filename=tmp_path)
        ws = gc.open_by_key(sheet_id).get_worksheet(0)
        rows = ws.get_all_records()   # list of dicts keyed by header row

        # The GSheet header row must contain 'Final Ad unit' (col E) and the
        # enriched cols (cols K-S).  We build a name → enriched dict.
        lookup = {}
        for row in rows:
            name = str(row.get("Final Ad unit", "")).strip()
            if not name:
                continue
            lookup[name] = {c: row.get(c, "") for c in ENRICHED_COLS}

        logger.info(f"read_master_sheet: loaded {len(lookup)} existing entries")
        return lookup
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)


@task
def enrich_units(all_gam_data: list, master_lookup: dict) -> list:
    """
    Enrich every unit in all_gam_data with ENRICHED_COLS.
    Returns the same list with enriched fields added to each dict.
    """
    logger = get_run_logger()
    enriched = [enrich_unit(u, master_lookup) for u in all_gam_data]
    fully_done = sum(
        1 for u in enriched
        if all(u.get(c, "") not in ("", None) for c in ENRICHED_COLS)
    )
    logger.info(
        f"enrich_units: {len(enriched)} total | "
        f"{fully_done} fully enriched | "
        f"{len(enriched) - fully_done} partial (need manual review)"
    )
    return enriched


@task
def sync_to_master_by_id(enriched_data: list, sheet_id: str) -> list:
    """
    Append new ad units (with enriched metadata) to the master GSheet.
    Deduplication is by Final Ad unit ID (column I).

    Columns written (in order, matching the master sheet layout):
      A: Source
      B: Ad unit 1
      C: Ad unit 2
      D: Ad unit 3
      E: Final Ad unit
      F: Ad unit 1 ID
      G: Ad unit 2 ID
      H: Ad unit 3 ID
      I: Final Ad unit ID
      J: Status
      K: Expresso Website Name
      L: Business
      M: Site
      N: Platform
      O: Ad_Type
      P: Ad_Position
      Q: Ad_Position_Granular
      R: Innovation
      S: Section Names
    """
    import gspread
    logger = get_run_logger()
    auth = Secret.load("newgamkey").get()
    if isinstance(auth, dict): auth = json.dumps(auth)
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(auth); tmp_path = tmp.name
    try:
        gc = gspread.service_account(filename=tmp_path)
        sh = gc.open_by_key(sheet_id)
        ws = sh.get_worksheet(0)

        # Dedup: read existing Final Ad unit IDs from col I (col_values(9))
        raw_ids = ws.col_values(9)
        existing_ids = {str(v).strip().replace("'", "") for v in raw_ids if v}

        to_append = [
            u for u in enriched_data
            if str(u['Final Ad unit ID']).strip().replace("'", "") not in existing_ids
        ]

        if to_append:
            rows = [
                [
                    u['Source'],
                    u['Ad unit 1'],    u['Ad unit 2'],    u['Ad unit 3'],
                    u['Final Ad unit'],
                    u['Ad unit 1 ID'], u['Ad unit 2 ID'], u['Ad unit 3 ID'],
                    u['Final Ad unit ID'],
                    u['Status'],
                    # ── Enriched metadata columns ──
                    u.get('Expresso Website Name', ''),
                    u.get('Business', ''),
                    u.get('Site', ''),
                    u.get('Platform', ''),
                    u.get('Ad_Type', ''),
                    u.get('Ad_Position', ''),
                    u.get('Ad_Position_Granular', ''),
                    u.get('Innovation', ''),
                    u.get('Section Names', ''),
                ]
                for u in to_append
            ]
            ws.append_rows(rows, value_input_option='USER_ENTERED')
            logger.info(f"sync_to_master_by_id: appended {len(to_append)} new rows")
        else:
            logger.info("sync_to_master_by_id: no new rows to append")

        return to_append
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)


@task
def find_direct_order_gaps_by_id(all_gam_data: list, sheet_id: str) -> list:
    import gspread
    logger = get_run_logger()
    auth = Secret.load("newgamkey").get()
    if isinstance(auth, dict): auth = json.dumps(auth)
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(auth); tmp_path = tmp.name
    try:
        gc = gspread.service_account(filename=tmp_path)
        sh = gc.open_by_key(sheet_id)
        try:
            ws = sh.worksheet("Ad_Unit_Mapping")
        except gspread.exceptions.WorksheetNotFound:
            logger.warning("Tab 'Ad_Unit_Mapping' not found, falling back to first tab")
            ws = sh.get_worksheet(0)

        raw_values = ws.col_values(8)
        direct_order_ids = {
            str(v).strip().replace("'", "")
            for v in raw_values if v
        }
        logger.info(f"find_direct_order_gaps: indexed {len(direct_order_ids)} IDs from Ad_Unit_Mapping")

        gaps = [
            u for u in all_gam_data
            if str(u['Final Ad unit ID']).strip().replace("'", "") not in direct_order_ids
        ]
        logger.info(f"find_direct_order_gaps: {len(gaps)} units missing from Ad_Unit_Mapping")
        return gaps
    except Exception as e:
        logger.error(f"CRITICAL ERROR in gap check: {e}")
        return []
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)


@task
def send_combined_email(added_to_master: list, direct_order_gaps: list):
    logger = get_run_logger()
    sender   = "ritik.jain@timesinternet.in"
    pwd      = Secret.load("gmaillogin").get()
    to_recip = "colombia.pubops@timesinternet.in"
    cc_recip = "ritik.jain@timesinternet.in"

    now = datetime.now()
    d = now.day
    suffix = 'th' if 11 <= d <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(d % 10, 'th')
    date_str = now.strftime(f"{d}{suffix} %B %Y")

    msg = MIMEMultipart()
    msg['Subject'] = f"GAM Sync Report - {date_str}"
    msg['From']    = sender
    msg['To']      = to_recip
    msg['Cc']      = cc_recip

    body = f"Hello,\n\nAutomated GAM sync report for {date_str}:\n\n"

    if added_to_master:
        body += (f"✅ Added to Master: {len(added_to_master)} new ad units "
                 f"(see attached CSV — includes enriched metadata).\n")
    else:
        body += "ℹ️ Master Sheet: No new ad units found.\n"

    if direct_order_gaps:
        body += (f"⚠️ Direct Order Sheet: {len(direct_order_gaps)} units missing "
                 f"(see attached CSV — includes suggested metadata for each unit).\n")
    else:
        body += "✅ Direct Order Sheet: Fully maintained, no gaps.\n"

    body += "\nAutomated by Prefect."
    msg.attach(MIMEText(body, 'plain'))

    # ── CSV builder ──────────────────────────────────────────────────────────
    # Both attachments include the enriched columns so ops can act immediately.
    ATTACH_FIELDS = [
        "Source", "Final Ad unit", "Final Ad unit ID",
        "Expresso Website Name", "Business", "Site", "Platform",
        "Ad_Type", "Ad_Position", "Ad_Position_Granular",
        "Innovation", "Section Names",
    ]

    def attach_csv(data: list, fname: str):
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=ATTACH_FIELDS, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(buf.getvalue().encode('utf-8'))
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{fname}"')
        msg.attach(part)

    if added_to_master:
        attach_csv(added_to_master, f"New_Ad_Units_{date_str}.csv")
    if direct_order_gaps:
        attach_csv(direct_order_gaps, f"Direct_Order_Gaps_{date_str}.csv")

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=30) as server:
            server.login(sender, pwd)
            server.sendmail(sender, [to_recip, cc_recip], msg.as_string())
        logger.info("Email sent successfully")
    except Exception as e:
        logger.error(f"Email error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 ── MASTER FLOW
# ═══════════════════════════════════════════════════════════════════════════════

@flow(log_prints=True)
def run_ad_unit_dump():
    install_dependencies()

    MASTER_SHEET_ID  = "1c6T7qbisk93oyABaoQIPc5Mny2P6h3ter47toAKff_w"
    DIRECT_ORDER_ID  = "1r6qaWp3JB5f4Zxd3wMYcEuq4gMmCntaKRKklEgnRulc"

    configs = [
        {
            'label': 'OLD GAM', 'network_code': '7176',
            'status_filter': 'ACTIVE',
            'target_ids': [23325198618, 23326563038],
            'depth': 5, 'skip_levels': 1,
        },
        {
            'label': 'New GAM', 'network_code': '23037861279',
            'status_filter': None, 'target_ids': None,
            'depth': 6, 'skip_levels': 2,
        },
    ]

    # 1. Fetch from both GAMs
    all_gam_data = []
    for cfg in configs:
        all_gam_data.extend(fetch_gam_data(cfg))

    # 2. Load master sheet for enricher lookup
    master_lookup = read_master_sheet(MASTER_SHEET_ID)

    # 3. Enrich all units (exact match → rules)
    enriched_data = enrich_units(all_gam_data, master_lookup)

    # 4. Append new units (with enriched cols) to master
    added_to_master = sync_to_master_by_id(enriched_data, MASTER_SHEET_ID)

    # 5. Check direct order gaps (enriched data used so email has metadata)
    direct_order_gaps = find_direct_order_gaps_by_id(enriched_data, DIRECT_ORDER_ID)

    # 6. Email report
    send_combined_email(added_to_master, direct_order_gaps)


if __name__ == "__main__":
    run_ad_unit_dump()