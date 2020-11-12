from src.content import Application, Book, Game, Movie, Serie, Track

from threading import Thread
from datetime import datetime
from flask import current_app
from abc import ABCMeta, abstractmethod


class Engine(Thread, metaclass=ABCMeta):
    __media__ = [Application, Book, Game, Movie, Serie, Track]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app = current_app._get_current_object()
        self.logger = current_app.logger

    def run(self):
        st_time = datetime.utcnow()
        with self.app.app_context():
            self.train()
        self.logger.info("%s engine performed in %s" %
                         (self.__class__.__name__, datetime.utcnow()-st_time))

    @abstractmethod
    def train(self):
        pass
