from src.content import Application, Book, Game, Movie, Serie, Track
from src.utils import db

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

    def store_date(self, content_type):
        content_type = str(content_type).upper()
        with db as session:
            res = session.execute('INSERT INTO "engine" VALUES (\'%s\', current_timestamp, \'%s\') ' % (self.__class__.__name__, content_type) +
                                  'ON CONFLICT ON CONSTRAINT engine_pkey DO ' +
                                  'UPDATE SET last_launch_date = current_timestamp WHERE "engine".engine = \'%s\' AND "engine".content_type = \'%s\'' % (self.__class__.__name__, content_type))

    @abstractmethod
    def train(self):
        pass

    def check_if_necessary(self):
        raise Exception("check_if_necessary method should be implemented")
