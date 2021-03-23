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
        self.state = {key: value}
        self.storage.save_state(self.state)
        return self.state

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
            return state


def test_json():
    json_st = JsonFileStorage()
    print('RETRIEVE_STATE before', json_st.retrieve_state())
    print('SAVE_STATE', json_st.save_state({"lol": "kek"}))
    print('RETRIEVE_STATE after', json_st.retrieve_state())


def test_state():
    my_state = State()
    print('SET_STATE', my_state.set_state('he', 'ha'))
    print('STATE', my_state.state)
    print('GET_STATE', my_state.get_state('he'))


if __name__ == '__main__':
    test_json()
    test_state()

