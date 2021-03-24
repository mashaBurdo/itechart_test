"""
1. Storage is saved as a dict
2. Data are saved to file as a json
3. If no key - then None
4. If file is empty - empty dict
5. After getting key-value pair it is saved to file
"""
import json
from redis import Redis
from backoff_decorator import backoff


FILE_PATH = "state.json"


def dict_to_redis_hset(r, hkey, dict_to_store):
    return all([r.hset(hkey, k, v) for k, v in dict_to_store.items()])


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
        with open(self.file_path, "r") as file:
            try:
                data = json.load(file)
                return data
            except json.JSONDecodeError:
                return {}

    @backoff()
    def save_state(self, state):
        with open(self.file_path, "r+") as file:
            data = json.load(file)
            data.update(state)
            file.seek(0)
            json.dump(data, file)
            return state


class RedisStorage:
    def __init__(self, redis_adapter=Redis("localhost")):
        self.redis_adapter = redis_adapter

    @backoff()
    def retrieve_state(self):
        try:
            data = self.redis_adapter.hgetall("Storage")
            return data
        except:
            return {}

    @backoff()
    def save_state(self, state):
        dict_to_redis_hset(self.redis_adapter, "Storage", state)
        return state


def test_json():
    json_st = JsonFileStorage()
    print("JSON RETRIEVE_STATE before", json_st.retrieve_state())
    print("JSON SAVE_STATE", json_st.save_state({"ol": "kek"}))
    print("JSON RETRIEVE_STATE after", json_st.retrieve_state())


def test_state():
    my_state = State()
    print("SET_STATE", my_state.set_state("he", "ha"))
    print("STATE", my_state.state)
    print("GET_STATE", my_state.get_state("he"))


def test_redis():
    r = RedisStorage()
    print("REDIS RETRIEVE_STATE before", r.retrieve_state())
    print("REDIS SAVE_STATE", r.save_state({"lol": "kek"}))
    print("REDIS RETRIEVE_STATE after", r.retrieve_state())


if __name__ == "__main__":
    test_state()
    test_json()
    test_redis()
