import re
from typing import Optional

# ── Platform Discriminators ────────────────────────────────────────────────
_WEB_PLATFORMS = {"Web"}
_MOBILE_PLATFORMS = {"Mweb", "AMP", "IOS", "AOS", "CTV", "FireOS", "OTT"}

# ── Platform Split Logic ───────────────────────────────────────────────────
_PLATFORM_SPLIT: dict[str, tuple] = {
    "ATF": (("Leaderboard", "ATF", "ATF", "TIL_Leaderboard"), ("Top Banner", "ATF", "ATF", "TIL_Top Banner, Page Push Down, WAP Page Push Down")),
    "BTF": (("Leaderboard", "BTF", "BTF", "TIL_Leaderboard"), ("Bottom Overlay", "ATF", "ATF", "TIL_Bottom Overlay")),
    "BO":  (("Leaderboard", "BTF", "BTF", "TIL_Leaderboard"), ("Bottom Overlay", "ATF", "ATF", "TIL_Bottom Overlay")),
}

_ATF_VARIANTS = {"ATF_P1", "ATF_P2", "ATF_M1", "ATF_M1_P1", "ATF_M2", "ATF_M2_P1", "ATF_M3", "ATF_M3_P1", "ATF_R1", "ATF_R2", "ATF_R3", "ATF_REF"}
_BTF_VARIANTS = {"BTF_P1", "BTF_P2", "BTF_M1", "BTF_M2", "BTF_M3", "BTF_R1", "BTF_R2", "BTF_R3", "BTF_INF", "BTF_1", "BTF_2", "BTF_3", "BTF_4", "BTF_5", "BO_M1", "BO_M2", "BO_M3", "BO_P1", "BO_R1", "BO_R2", "BO_R3", "BO_REF", "BO_CR1", "BO_CR2", "BO_CR_1", "BO_CR_2"}
_BO_VARIANTS = {"BTF_REF"}

# ── Suffix Mapping ─────────────────────────────────────────────────────────
_SUFFIX_MAP: dict[str, tuple] = {
    "ATF_1":        ("Leaderboard", "ATF", "ATF", ""),
    "ATF_2":        ("Leaderboard", "ATF", "ATF", ""),
    "ATF_A1":       ("Leaderboard", "ATF", "ATF", "TIL_Leaderboard,TIL_Billboard"),
    "ATF_A2":       ("Leaderboard", "ATF", "ATF", "TIL_Leaderboard,TIL_Billboard"),
    "ATF_PPD":      ("Leaderboard", "ATF", "ATF", "TIL_Feature Banner"),
    "AFF_ATF":      ("Leaderboard", "ATF", "ATF", "TIL_Leaderboard"),
    "SW":           ("Leaderboard", "ATF", "ATF", "TIL_Leaderboard"),
    "LB":           ("Leaderboard", "ATF", "ATF", "TIL_Leaderboard"),
    "BTF_INF":      ("Leaderboard", "BTF", "BTF", "TIL_Leaderboard"),
    "AFF_BO":       ("Bottom Overlay", "ATF", "ATF", "TIL_Bottom Overlay"),
    "INT":          ("Interstitial", "Out Of Page", "Out Of Page", "TIL_Interstitial"),
    "INT_P1":       ("Interstitial", "Out Of Page", "Out Of Page", "TIL_Interstitial"),
    "INT_PRE":      ("Interstitial", "Out Of Page", "Out Of Page", "TIL_Interstitial"),
    "INT_1":        ("Interstitial", "Out Of Page", "Out Of Page", "Google Viggnete"),
    "INT_2":        ("Interstitial", "Out Of Page", "Out Of Page", "Google Viggnete"),
    "INT_3":        ("Interstitial", "Out Of Page", "Out Of Page", "Google Viggnete"),
    "INT_4":        ("Interstitial", "Out Of Page", "Out Of Page", "Google Viggnete"),
    "INT_5":        ("Interstitial", "Out Of Page", "Out Of Page", "TIL_Interstitial"),
    "GOOGLE_INTERSTITIAL": ("Interstitial", "Out Of Page", "Out Of Page", "TIL_Interstitial"),
    "INNOV":        ("Skinner", "Out Of Page", "Out Of Page", "TIL_Skin"),
    "INNOV_M1":     ("Skinner", "Out Of Page", "Out Of Page", "TIL_Skin"),
    "INNOV_M2":     ("Skinner", "Out Of Page", "Out Of Page", "TIL_Skin"),
    "INNOV_M3":     ("Skinner", "Out Of Page", "Out Of Page", "TIL_Skin"),
    "INNOV_A1":     ("Skinner", "Out Of Page", "Out Of Page", "TIL_Skin"),
    "INNOV_A2":     ("Skinner", "Out Of Page", "Out Of Page", "TIL_Skin"),
    "AFF_INNOV":    ("Skinner", "Out Of Page", "Out Of Page", "Skin,TIL_Skin"),
    "SKINNER_LHS":  ("Skinner", "LHS", "LHS", ""),
    "SKINNER_RHS":  ("Skinner", "RHS", "RHS", ""),
    "EARPANEL_LHS": ("Earpanel", "LHS", "LHS", "TIL_Ear Panel LHS"),
    "EARPANEL_RHS": ("Earpanel", "RHS", "RHS", "TIL_Ear Panel RHS"),
    "FC":           ("Flying Carpet", "BTF", "BTF", "TIL_Flying Carpet"),
    "FC_P1":        ("Flying Carpet", "BTF", "BTF", "TIL_Flying Carpet"),
    "FC_P2":        ("Flying Carpet", "BTF", "BTF", "TIL_Flying Carpet"),
    "FC_REF":       ("Flying Carpet", "BTF", "BTF", "TIL_Flying Carpet"),
    "TOPBAND":      ("Top Band", "ATF", "ATF", "TIL_Top Band"),
    "BEACON":       ("Beacon", "ATF", "ATF", "TIL_Beacon"),
    "BEACON_1X1":   ("Beacon", "ATF", "ATF", "TIL_Beacon"),
    "CUBELOGO_1":   ("Cube", "ATF", "ATF", "TIL_Cube"),
    "CUBELOGO_2":   ("Cube", "ATF", "ATF", "TIL_Cube"),
    "CUBETEXT_1":   ("Cube", "ATF", "ATF", "TIL_Cube"),
    "CUBETEXT_2":   ("Cube", "ATF", "ATF", "TIL_Cube"),
    "STORYAD":      ("Auto Ads", "ATF", "ATF", "TIL_Interstitial"),
    "STORYAD_M1":   ("Auto Ads", "ATF", "ATF", "TIL_Interstitial"),
    "STORYAD_M2":   ("Auto Ads", "ATF", "ATF", "TIL_Interstitial"),
    "STORYAD_M3":   ("Auto Ads", "ATF", "ATF", "TIL_Interstitial"),
    "LBAND":        ("LBand", "ATF", "ATF", "L-Band"),
    "MINITV_LBAND": ("Lband", "ATF", "ATF", "L-Band"),
    **{f"SLUG_{i}": ("Slug", "BTF", "BTF", "TIL_Slug") for i in range(1, 11)},
    "SLUGFLIP_1":   ("Slug", "BTF", "BTF", "TIL_Slug"),
    "SLUGFLIP_2":   ("Slug", "BTF", "BTF", "TIL_Slug"),
    **{f"MTF_{i}":  ("Mrec", "MTF", "MTF Mrec", "Mrec,TIL_Mrec,TIL_In-banner Video") for i in range(1, 6)},
    "MTF_INF":      ("Mrec", "MTF", "INF Mrec", "Mrec,TIL_Mrec,TIL_In-banner Video"),
    "PREROLL":      ("Video", "Preroll", "Preroll", "TIL_Instream Pre-Roll User Initiated"),
    "POSTROLL":     ("Video", "Postroll", "ATF", "TIL_Instream Pre-Roll User Initiated"),
    "MIDROLL":      ("Video", "Midroll", "1st Midroll", "TIL_Instream Pre-Roll User Initiated"),
}

# ── Base Maps & Regex ──────────────────────────────────────────────────────
_ORDINAL = {1: "ATF", 2: "2nd Mrec", 3: "3rd Mrec", 4: "4th Mrec", 5: "5th Mrec", 6: "6th Mrec", 7: "7th Mrec", 8: "8th Mrec", 9: "9th Mrec", 10: "10th Mrec"}
_MIDORD = {1: "1st Midroll", 2: "2nd Midroll", 3: "3rd Midroll"}
_MREC_RE   = re.compile(r"^(AFF_)?MREC_(\d+|INF)(_(P[12]|R[123]|M[123]|A[12]|CR[12]|REF|HD))*$", re.I)
_VIDEO_RE  = re.compile(r"^(.+?)_(PREROLL|POSTROLL|MIDROLL_?(\d*))$", re.I)
_MTF_RE    = re.compile(r"^MTF_(\d+|INF)$", re.I)
_CAN_RE    = re.compile(r"^(.+)_CAN$", re.I)

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
    "HOME": "HP,Home", "US": "HP,Home", "PSBK": "PASSBACK",
    "IPL": "IPL, CRICKET, SPORTS, RoS", "CRICKET": "CRICKET, SPORTS, RoS", "SPORTS": "SPORTS, RoS",
}

_AU1_KEYS_SORTED = sorted(_AD_UNIT_1_MAP.keys(), key=len, reverse=True)

# ── Resolution Functions ───────────────────────────────────────────────────

def _resolve_au1(au1: str) -> Optional[tuple]:
    key_upper = au1.upper()
    for k in _AU1_KEYS_SORTED:
        if k.upper() == key_upper:
            return _AD_UNIT_1_MAP[k]
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
    if len(parts) < 3: return ""
    topic = parts[2].upper()
    return _SPECIAL_SECTION.get(topic, f"{topic}, RoS")

def _resolve_suffix(suffix: str, platform: str) -> Optional[tuple]:
    upper = suffix.upper()
    is_web = platform in _WEB_PLATFORMS

    if upper in _PLATFORM_SPLIT:
        return _PLATFORM_SPLIT[upper][0] if is_web else _PLATFORM_SPLIT[upper][1]

    if upper in {v.upper() for v in _ATF_VARIANTS}:
        return _PLATFORM_SPLIT["ATF"][0] if is_web else _PLATFORM_SPLIT["ATF"][1]

    if upper in {v.upper() for v in _BTF_VARIANTS}:
        return _PLATFORM_SPLIT["BTF"][0] if is_web else _PLATFORM_SPLIT["BTF"][1]

    if upper == "BTF_REF":
        return ("Leaderboard", "BTF", "BTF", "TIL_Leaderboard") if is_web else ("Bottom Overlay", "ATF", "ATF", "TIL_Bottom Overlay")

    for k, v in _SUFFIX_MAP.items():
        if k.upper() == upper: return v

    if _MREC_RE.match(upper):
        core = re.sub(r"^AFF_", "", upper)
        num_part = core.split("_")[1]
        if num_part == "INF": gran, pos = "INF Mrec", "BTF"
        else:
            n = int(num_part)
            gran = _ORDINAL.get(n, f"{n}th Mrec") if n > 1 else "ATF"
            pos  = "ATF" if n == 1 else "BTF"
        return ("Mrec", pos, gran, "Mrec,TIL_Mrec,TIL_In-banner Video")

    if _MTF_RE.match(upper):
        return ("Mrec", "MTF", "MTF Mrec", "Mrec,TIL_Mrec,TIL_In-banner Video")

    m = _VIDEO_RE.match(upper)
    if m:
        action = m.group(2).upper()
        if "PREROLL" in action: return ("Video", "Preroll", "Preroll", "TIL_Instream Pre-Roll User Initiated")
        if "POSTROLL" in action: return ("Video", "Postroll", "ATF", "TIL_Instream Pre-Roll User Initiated")
        if "MIDROLL" in action:
            n = int(m.group(3)) if m.group(3) else 1
            gran = _MIDORD.get(n, f"{n}th Midroll")
            return ("Video", "Midroll", gran, "TIL_Instream Pre-Roll User Initiated")
    return None

# ── Main Entry Function ────────────────────────────────────────────────────
def apply_comprehensive_logic(unit: dict) -> dict:
    """Takes a dictionary containing Ad unit 1, 2, 3, 4 (Final Ad unit) and extracts all 8 columns."""
    ad4 = str(unit.get("Final Ad unit", "")).strip()
    enriched = {
        "Expresso Website Name": "", "Business": "", "Site": "", "Platform": "",
        "Ad_Type": "", "Ad_Position": "", "Ad_Position_Granular": "", 
        "Innovation": "", "Section Names": ""
    }
    if not ad4: return enriched

    ad4_upper = ad4.upper()
    matched_au1 = None
    for key in _AU1_KEYS_SORTED:
        if ad4_upper.startswith(key.upper() + "_") or ad4_upper == key.upper():
            matched_au1 = key
            break

    if matched_au1:
        au1_attrs = _resolve_au1(matched_au1)
        if au1_attrs:
            enriched["Platform"] = au1_attrs[0]
            enriched["Business"] = au1_attrs[1]
            enriched["Site"] = au1_attrs[2]
            enriched["Expresso Website Name"] = au1_attrs[3]

    platform = enriched["Platform"]

    au2 = str(unit.get("Ad unit 2", "")).strip()
    if au2: enriched["Section Names"] = _infer_section(au2)

    if matched_au1:
        au1_parts = len(matched_au1.split("_"))
        all_parts = ad4.split("_")
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

    return enriched