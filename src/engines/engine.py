from threading import Thread
from datetime import datetime
from flask import current_app
from abc import ABCMeta, abstractmethod


class Engine(Thread, metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app = current_app._get_current_object()
        self.logger = current_app.logger

    def run(self):
        st_time = datetime.utcnow()
        with self.app.app_context():
            self.load()
        self.logger.info("%s engine performed in %s" %
                         (self.__class__.__name__, datetime.utcnow()-st_time))

    @abstractmethod
    def load(self):
        pass
