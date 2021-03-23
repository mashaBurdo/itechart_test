"""
1. Storage is saved as a dict
2. Data are saved to file as a json
3. If no key - then None
4. If file is empty - empty dict
5. After getting key-value pair it is saved to file
"""
import json
from backoff_decorator import backoff


FILE_PATH = 'state.json'


class State:
    """Implements state recovery when app starts, if such state existed"""
    def __init__(self):
        self.storage = JsonFileStorage()
        self.state = self.retrieve_state()

    def retrieve_state(self):
        state = self.storage.retrieve_state()
        return state

    def set_state(self, key, value):
        self.storage.save_state({key: value})
        return {key: value}

    def get_state(self, key):
        value = self.state.get(key)
        return value

#
# class Storage:
#     def __init__(self):
#         pass
#
#     def retrieve_state(self):
#         """Gets state of permanent storage. Returns empty dict if storage is empty"""
#         pass
#
#     def save_state(self):
#         pass


class JsonFileStorage:
    """Save data in JSON to file_path and load data from file"""
    def __init__(self, file_path=FILE_PATH):
        self.file_path = file_path

    @backoff()
    def retrieve_state(self):
        with open(self.file_path, 'r') as file:
            try:
                data = json.load(file)
                return data
            except json.JSONDecodeError:
                return {}

    @backoff()
    def save_state(self, state):
        with open(self.file_path, 'w') as file:
            json.dump(state, file)


json_st = JsonFileStorage()
print(json_st.retrieve_state())
json_st.save_state({"lol": "kek"})
print(json_st.retrieve_state())


my_state = State()
print(my_state.set_state('he', 'ha'))
print(my_state.get_state('he')) # TODO: Why none?????
