"""
1. Storage is saved as a dict
2. Data re saved to file as a json
3. If no key - then None
4. If file is empty - empty dict
5. After getting key-value pair it is saved to file
"""
import json


class State:
    """Implements state recovery when app starts, if such state existed"""
    def __init__(self):
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
    def __init__(self):
        pass