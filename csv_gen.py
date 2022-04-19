from faker import Faker
import csv


def gen(count: int, file: str):
    headers = ["pk", "Name", "Job", "Company"]

    with open(file, "wt") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()

        faker = Faker()

        for i in range(0, count):
            writer.writerow(
                {
                    "pk": i,
                    "Name": faker.name(),
                    "Job": faker.job(),
                    "Company": faker.company(),
                }
            )


if __name__ == "__main__":
    gen(100000, "large_test.csv")
