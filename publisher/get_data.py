import json
class GetData:

    def get_json(self, path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data
        