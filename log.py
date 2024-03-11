import pymongo
import json
import datetime
import os

# Connect
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["logs"]
collection = db["log_entries"]

# Specify the folder containing JSON files
folder_path = "data/log_data/2018/11"

# Iterate files
week=1
day=1
for log_file in os.listdir(folder_path):
    if log_file.endswith(".json"):
        file_path = os.path.join(folder_path, log_file)
        try:
            with open(file_path, "r") as file:  
                #Iterate data in Json
                for line in file:
                    try:
                        entry = json.loads(line)
                        entry ['week'] = week
                        entry["timestamp"] = datetime.datetime.now()
                        collection.insert_one(entry)
                        
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON in file {log_file}, line: {line.strip()}: {e}")
            
            print(f"Log data from {log_file} inserted into MongoDB.")
        except FileNotFoundError:
            print(f"File {file_path} not found.")
        except Exception as e:
            print(f"An error occurred: {e}")
    day+=1
    if day ==8:
        week += 1
        day = 1

print("All log data inserted into MongoDB successfully.")
