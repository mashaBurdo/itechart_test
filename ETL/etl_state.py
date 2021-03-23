"""
DONE 1. Storage is saved as a dict
DONE 2. Data are saved to file as a json
3. If no key - then None
DONE 4. If file is empty - empty dict
5. After getting key-value pair it is saved to file
"""
import json
from backoff_decorator import backoff


FILE_PATH = 'state.json'


class State:
    """Implements state recovery when app starts, if such state existed
    WORKS WITH LOCAL STORAGE"
    needs a storage parametr"""
    def __init__(self, file_path):
        pass

    def set_state(self):
        pass

    def get_state(self):
        pass


class Storage:
    def __init__(self):
        pass

    def retrieve_state(self):
        """Gets state of permanent storage. Returns empty dict if storage is empty"""
        pass

    def save_state(self):
        pass


class JsonFileStorage:
    """Save data in JSON to file_path and load data from file"""
    def __init__(self, file_path=FILE_PATH):
        self.file_path = file_path

    @backoff()
    def load_data(self):
        with open(self.file_path, 'r') as file:
            try:
                data = json.load(file)
                return data
            except json.JSONDecodeError:
                return {}

    @backoff()
    def dump_data(self, state):
        with open(self.file_path, 'w') as file:
            json.dump(state, file)


json_st = JsonFileStorage()
print(json_st.load_data())
json_st.dump_data({"ha": "ha"})
print(json_st.load_data())
