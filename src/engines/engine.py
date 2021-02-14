from src.content import Application, Book, Game, Movie, Serie, Track

from threading import Thread
from datetime import datetime
from flask import current_app
from abc import ABCMeta, abstractmethod

import traceback


class Engine(Thread, metaclass=ABCMeta):
    __media__ = [Application, Book, Game, Movie, Serie, Track]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app = current_app._get_current_object()
        self.logger = current_app.logger
        self.status = None
        self.error = None

    def run(self):
        st_time = datetime.utcnow()
        with self.app.app_context():
            try:
                self.train()
            except Exception as e:
                traceback.print_exc()
                self.logger.error("Exception %s", e)
                self.status = False
                self.error = e
            else:
                self.status = True
        self.logger.info("%s engine performed in %s" %
                         (self.__class__.__name__, datetime.utcnow()-st_time))

    @abstractmethod
    def train(self):
        pass
