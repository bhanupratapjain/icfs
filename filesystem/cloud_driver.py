from abc import abstractmethod


class CloudDriver:
    @abstractmethod
    def pull(self, name):
        pass

    @abstractmethod
    def push(self, name):
        pass

    @abstractmethod
    def delete(self, name):
        pass
