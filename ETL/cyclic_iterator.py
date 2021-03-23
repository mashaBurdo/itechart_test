import time


class CyclicIterator:
    def __init__(self, iterable):
        self.restart = iterable
        self.iterable = iter(iterable)

    def __iter__(self):
        self.iterable
        return self

    def __next__(self):
        while True:
            try:
                item = next(self.iterable)
                return item
            except StopIteration:
                self.__init__(self.restart)


cyclic_iterator = CyclicIterator(["a", "b", "c"])
for i in cyclic_iterator:
    print(i)
    time.sleep(0.5)
