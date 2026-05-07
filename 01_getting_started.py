from prefect import flow, task

@task
def say_hello(name: str):
    print(f"Hello {name}!")

@flow(log_prints=True)
def main(name: str = "World"):
    say_hello(name)

if __name__ == "__main__":
    main()
