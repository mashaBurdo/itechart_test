"""
1. Storage is saved as a dict
2. Data are saved to file as a json
3. If no key - then None
4. If file is empty - empty dict
5. After getting key-value pair it is saved to file
"""
import json
from redis import StrictRedis


FILE_PATH = "state.json"


class JsonFileStorage:
    """Save data in JSON to file_path and load data from file"""

    def __init__(self, file_path=FILE_PATH):
        self.file_path = file_path

    def retrieve_state(self):
        with open(self.file_path, "r") as file:
            try:
                data = json.load(file)
                return data
            except json.JSONDecodeError:
                return {}

    def save_state(self, state):
        data = {}
        with open(self.file_path, "r") as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError:
                data = {}

        with open(self.file_path, "w") as file:
            key, value = list(state.items())[0]
            data[key] = value
            json.dump(data, file)
            return state


    def clear_state(self):
        with open(self.file_path, "w") as file:
            data = {}
            json.dump(data, file)
            return data


class RedisStorage:
    def __init__(self, redis_adapter=StrictRedis(host='redis', port=6379, db=0)):
        self.redis_adapter = redis_adapter

    def retrieve_state(self):
        try:
            data = self.redis_adapter.hgetall("Storage")

            data = {key.decode('utf-8'): json.loads(value) for key, value in data.items()}
            if data:
                return data
            else:
                return {}
        except:
            return {}

    def save_state(self, state):
        key, value = list(state.items())[0]
        json_state = json.dumps(value)
        self.redis_adapter.hset("Storage", key, json_state)
        return state

    def clear_state(self):
        self.redis_adapter.flushdb()
        return {}


class State:
    """Implements state recovery when app starts, if such state existed"""

    def __init__(self, storage=RedisStorage()):
        self.storage = storage
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

    def clear_state(self):
        value = self.storage.clear_state()
        self.state = self.storage.retrieve_state()
        return value


def test_json():
    json_st = JsonFileStorage()
    print("JSON RETRIEVE_STATE before", json_st.retrieve_state())
    print("JSON SAVE_STATE", json_st.save_state({"ol": "kek"}))
    print("JSON RETRIEVE_STATE after", json_st.retrieve_state())
    print("JSON CLEAR_STATE", json_st.clear_state())
    print("JSON RETRIEVE_STATE cleared", json_st.retrieve_state())


def test_state():
    my_state = State()
    print("SET_STATE", my_state.set_state("he", "ha"))
    print("STATE", my_state.state)
    print("GET_STATE", my_state.get_state("he"))
    print("CLEAR_STATE", my_state.clear_state())
    print("GET_STATE", my_state.get_state("he"))
    print("STATE", my_state.state)


def test_redis():
    r = RedisStorage()
    print("REDIS RETRIEVE_STATE before", r.retrieve_state())
    print("REDIS SAVE_STATE", r.save_state({"lol": ['k', 'e', 'k']}))
    print("REDIS RETRIEVE_STATE after", r.retrieve_state())
    print("REDIS CLEAR_STATE", r.clear_state())
    print("REDIS RETRIEVE_STATE cleared", r.retrieve_state())


# if __name__ == "__main__":
    # test_json()
    # test_state()
    # test_redis()
