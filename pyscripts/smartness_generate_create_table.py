import csv


if __name__ == '__main__':
    csv_file = "../dataset/X_cluster.csv"
    table_name = "smartness_keyspace.t300"

    with open(csv_file, newline='') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)

    columns = ", ".join(headers)
    values = ", ".join([f"\\\"{col}\\\" text" for col in headers[:300]])

    print(f"CREATE TABLE {table_name} (id UUID PRIMARY KEY, {values});")