import json

def json_to_file(name,data):
    with open(name, 'w') as f:
        json.dump(data, f)