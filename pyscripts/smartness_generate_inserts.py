import csv

if __name__ == '__main__':
    csv_file = "../dataset/X_cluster.csv"
    table_name = "smartness_keyspace.t300"

    with open(csv_file, newline='') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)

    columns = ", ".join([f"\\\"{col}\\\"" for col in headers[:100]])
    values = ", ".join([f"?" for col in headers[:100]])

    print(f"INSERT INTO {table_name} (\\\"id\\\", {columns}) VALUES (?, {values});")