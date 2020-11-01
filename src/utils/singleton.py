# https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
class Singleton(object):
    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            class_._instances[class_] = super(
                Singleton, cls).__new__(cls, *args, **kwargs)
        return cls._instances[cls]
