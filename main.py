from collections import defaultdict
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
        self.tables = defaultdict(dict)
        self.lock = threading.RLock()
        self.is_reorganizing = False
        self.reorganize_condition = threading.Condition(self.lock)
        self.request_times = []
        self.reorganize_times = []
        self.reorganize_interval = reorganize_interval
        self.pending_increments = defaultdict(lambda: defaultdict(int))
        self.load_data()
        self.shadow_tables = None  # Shadow database for reorganization
        self.reorganize_thread = None  # Background reorganization thread


    def load_data(self):
        if os.path.exists(self.db_file):
            try:
                with open(self.db_file, "r") as f:
                    self.tables.update(json.load(f))
            except json.JSONDecodeError as e:
                print(f"Error loading data from {self.db_file}: {e}")

    def save_data(self):
        try:
            with self.lock:
                temp_data = dict(self.tables)  # Create a copy for saving
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
        with self.lock:
            if self.is_reorganizing:
                print("Reorganization in progress. Reading from active tables.")
            row = self.tables.get(table_name, {}).get(row_id)
            return row if row else {"error": "Row not found."}


    def track_increment(self, table_name, row_id):
        with self.reorganize_condition:
            while self.is_reorganizing:
                self.reorganize_condition.wait()

        start_time = time.time()
        with self.lock:
            if table_name in self.tables and row_id in self.tables[table_name]:
                self.pending_increments[table_name][row_id] += 1
                self.record_request_time(start_time)
                return {"message": f"Increment tracked for row '{row_id}' in table '{table_name}'."}
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

    def start_reorganization(self):
        if self.reorganize_thread and self.reorganize_thread.is_alive():
            print("Reorganization already in progress.")
            return

        def reorganize():
            print("Starting asynchronous reorganization...")
            with self.lock:
                self.is_reorganizing = True
                self.shadow_tables = {table: dict(data) for table, data in self.tables.items()}  # Make a shadow copy

            # Reorganize the shadow copy
            start_time = time.time()
            for table_name, table_data in self.shadow_tables.items():
                self.shadow_tables[table_name] = dict(sorted(
                    table_data.items(),
                    key=lambda item: item[1].get("query_count", 0),
                    reverse=True
                ))

            # Swap the shadow copy with the active database
            with self.lock:
                self.tables = self.shadow_tables
                self.shadow_tables = None
                self.is_reorganizing = False
                self.reorganize_condition.notify_all()

            elapsed = time.time() - start_time
            print(f"Asynchronous reorganization completed in {elapsed:.2f} seconds.")

        self.reorganize_thread = threading.Thread(target=reorganize)
        self.reorganize_thread.start()



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

    def get_password(self, table_name, username):
        with self.reorganize_condition:
            while self.is_reorganizing:
                self.reorganize_condition.wait()

        with self.lock:
            user_row = self.tables.get(table_name, {}).get(username)
            if user_row and "password" in user_row:
                return {"username": username, "password": user_row["password"]}
            return {"error": f"Password not found for username '{username}'."}




    def start_combined_reorganization(self, partition_size=1000):
        #yuh!
        if self.reorganize_thread and self.reorganize_thread.is_alive():
            print("Reorganization already in progress.")
            return

        def reorganize():
            print("Starting combined reorganization...")
            with self.lock:
                self.is_reorganizing = True
                self.shadow_tables = {table: dict(data) for table, data in self.tables.items()}  # Shadow copy

            start_time = time.time()

            # Calculate table priorities based on total query count
            table_priorities = sorted(
                self.shadow_tables.keys(),
                key=lambda table_name: sum(
                    row.get("query_count", 0) for row in self.shadow_tables[table_name].values()
                ),
                reverse=True
            )

            # Reorganize tables in priority order, with partitioning
            for table_name in table_priorities:
                table_data = self.shadow_tables[table_name]
                keys = list(table_data.keys())
                for i in range(0, len(keys), partition_size):
                    partition_keys = keys[i:i + partition_size]
                    partition = {key: table_data[key] for key in partition_keys}
                    sorted_partition = dict(sorted(
                        partition.items(),
                        key=lambda item: item[1].get("query_count", 0),
                        reverse=True
                    ))
                    self.shadow_tables[table_name].update(sorted_partition)
                    #print(f"Reorganized partition {i} to {i + partition_size} in table '{table_name}'.")

            # Swap the reorganized shadow copy with the active database
            with self.lock:
                self.tables = self.shadow_tables
                self.shadow_tables = None
                self.is_reorganizing = False
                self.reorganize_condition.notify_all()

            elapsed = time.time() - start_time
            print(f"Combined reorganization completed in {elapsed:.2f} seconds.")

        self.reorganize_thread = threading.Thread(target=reorganize)
        self.reorganize_thread.start()



def generate_users(db, table_name, num_users=1_000_000, batch_size=10_000):
    db.add_table(table_name)

    def create_user(index):
        username = f"user_{index}"
        password = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
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


def simulate_traffic(db, table_name, num_queries, power_users_ratio=0.75):
    user_keys = list(db.tables.get(table_name, {}).keys())

    if not user_keys:
        print("No users to simulate traffic.")
        return

    power_users_count = int(len(user_keys) * power_users_ratio)
    power_users = random.sample(user_keys, power_users_count)

    start = time.time()
    for i in range(num_queries):

        start1 = time.time()
        if i > 0 and i % db.reorganize_interval == 0:
            print(f"Reorganizing database after {i} queries...")
            db.start_combined_reorganization()
        username = random.choice(power_users) if random.random() < power_users_ratio else random.choice(user_keys)
        password_info = db.get_password(table_name, username)
        #print(str(username) + " | " + str(password_info))
        db.track_increment(table_name, username)
        elapse1 = time.time()-start1
        if elapse1 > 0.001:
            print(f"Slow query detected: {elapse1:.6f} seconds") 

    db.save_data()
    end = time.time()
    print(f"Simulated {num_queries} queries in {end - start:.2f} seconds.")


if __name__ == "__main__":
    db = ExpandableDatabase()

    print("Starting user generation...")
    generate_users(db, table_name="users", num_users=1000000, batch_size=100000)
    print("User  generation complete!")

    print("Starting traffic simulation...")
    simulate_traffic(db, table_name="users", num_queries=10000000)
    print("Traffic simulation complete!")

    print(f"Average request time: {db.get_average_request_time():.20f} seconds")
    print(f"Maximum request time: {db.get_max_request_time():.20f} seconds")
    #Broken for now... :( soz
    print(f"Average reorganize time: {db.get_average_reorganize_time():.20f} seconds")
    print(f"Maximum reorganize time: {db.get_max_reorganize_time():.20f} seconds")
