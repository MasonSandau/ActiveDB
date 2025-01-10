import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import random
import string

class ExpandableDatabase:
    def __init__(self, db_file="database.json", reorganize_interval=1000000):
        self.db_file = db_file
        self.tables = {}
        self.lock = threading.RLock()
        self.request_times = []
        self.reorganize_times = []
        self.reorganize_interval = reorganize_interval
        self.load_data()

    def load_data(self):
        if os.path.exists(self.db_file):
            try:
                with open(self.db_file, "r") as f:
                    self.tables = json.load(f)
            except json.JSONDecodeError as e:
                print(f"Error loading data from {self.db_file}: {e}")
                self.tables = {}
        else:
            self.tables = {}

    def save_data(self):
        try:
            with self.lock:
                temp_data = self.tables.copy()
            with open(self.db_file, "w") as f:
                json.dump(temp_data, f, indent=4)
        except Exception as e:
            print(f"Error saving data to {self.db_file}: {e}")

    def add_table(self, table_name):
        with self.lock:
            if table_name in self.tables:
                return {"error": f"Table '{table_name}' already exists."}
            self.tables[table_name] = {}
            return {"message": f"Table '{table_name}' added successfully."}

    def add_row(self, table_name, row_id, row_data):
        with self.lock:
            if table_name not in self.tables:
                return {"error": f"Table '{table_name}' does not exist."}
            if row_id in self.tables[table_name]:
                return {"error": f"Row ID '{row_id}' already exists in table '{table_name}'."}
            self.tables[table_name][row_id] = row_data
            return {"message": f"Row ID '{row_id}' added to table '{table_name}' successfully."}

    def get_row(self, table_name, row_id):
        start_time = time.time()
        with self.lock:
            if table_name in self.tables and row_id in self.tables[table_name]:
                self.record_request_time(start_time)
                return self.tables[table_name][row_id]
            self.record_request_time(start_time)
            return {"error": "Row not found."}

    def increment_query_count(self, table_name, row_id):
        start_time = time.time()
        with self.lock:
            if table_name in self.tables and row_id in self.tables[table_name]:
                if "query_count" not in self.tables[table_name][row_id]:
                    self.tables[table_name][row_id]["query_count"] = 0
                self.tables[table_name][row_id]["query_count"] += 1
                self.record_request_time(start_time)
                return {"message": f"Query count incremented for row '{row_id}' in table '{table_name}'."}
            self.record_request_time(start_time)
            return {"error": "Row not found."}

    def record_request_time(self, start_time):
        elapsed_time = time.time() - start_time
        self.request_times.append(elapsed_time)

    def get_average_request_time(self):
        if not self.request_times:
            return 0
        return sum(self.request_times) / len(self.request_times)

    def get_max_request_time(self):
        if not self.request_times:
            return 0
        return max(self.request_times)

    def record_reorganize_time(self, start_time):
        elapsed_time = time.time() - start_time
        self.reorganize_times.append(elapsed_time)

    def get_average_reorganize_time(self):
        if not self.reorganize_times:
            return 0
        return sum(self.reorganize_times) / len(self.reorganize_times)

    def get_max_reorganize_time(self):
        if not self.reorganize_times:
            return 0
        return max(self.reorganize_times)

    def reorganize_database(self):
        print("Reorganizing database...")
        start_time = time.time()
        with self.lock:
            for table_name in self.tables:
                sorted_table = dict(sorted(
                    self.tables[table_name].items(),
                    key=lambda item: item[1].get("query_count", 0),
                    reverse=True
                ))
                self.tables[table_name] = sorted_table
        self.save_data()
        self.record_reorganize_time(start_time)
        print("Database reorganized.")

# Simulate traffic

def generate_users(db, table_name, num_users=1_000_000, batch_size=10_000):
    db.add_table(table_name)

    def create_user(index):
        username = f"user_{index}"
        password = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        db.add_row(table_name, username, {"password": password, "query_count": 0})

    start = time.time()
    with ThreadPoolExecutor() as executor:
        for batch_start in range(0, num_users, batch_size):
            batch_end = min(batch_start + batch_size, num_users)
            executor.map(create_user, range(batch_start, batch_end))
            print(f"Batch {batch_start} to {batch_end} added.")

    db.save_data()
    print("All users generated and saved.")
    print(f"Generated {num_users} users in {time.time() - start:.2f} seconds.")

def simulate_traffic(db, table_name, num_queries=1000, power_users_ratio=0.75):
    user_keys = list(db.tables.get(table_name, {}).keys())

    if not user_keys:
        print("No users to simulate traffic.")
        return

    power_users_count = int(len(user_keys) * power_users_ratio)
    power_users = random.sample(user_keys, power_users_count)

    start = time.time()
    for i in range(num_queries):
        if i > 0 and i % db.reorganize_interval == 0:
            print(f"Reorganizing database after {i} queries...")
            db.reorganize_database()

        if random.random() < power_users_ratio:
            username = random.choice(power_users)
        else:
            username = random.choice(user_keys)

        db.increment_query_count(table_name, username)

    db.save_data()
    end = time.time()
    print(f"Simulated {num_queries} queries in {end - start:.2f} seconds.")

if __name__ == "__main__":
    db = ExpandableDatabase()

    print("Starting user generation...")
    generate_users(db, table_name="users", num_users=1000000, batch_size=100000)
    print("User generation complete!")

    print("Starting traffic simulation...")
    simulate_traffic(db, table_name="users", num_queries=10000000)
    print("Traffic simulation complete!")

    print(f"Average request time: {db.get_average_request_time():.20f} seconds")
    print(f"Maximum request time: {db.get_max_request_time():.20f} seconds")
    print(f"Average reorganize time: {db.get_average_reorganize_time():.20f} seconds")
    print(f"Maximum reorganize time: {db.get_max_reorganize_time():.20f} seconds")
