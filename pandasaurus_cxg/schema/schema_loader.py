import json


def read_json_file(file_path):
    with open(file_path, "r") as file:
        json_data = json.load(file)
    return json_data
