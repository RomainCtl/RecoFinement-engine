from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import os
import sys
from flask import current_app

from settings import DB_URI
from .singleton import Singleton


class Database(Singleton):
    engine = create_engine(DB_URI)
    Session = sessionmaker(bind=engine)

    __current = None

    def get_new_session(self):
        return self.Session()

    def __enter__(self):
        # close session if for any reason, it is not
        if self.__current is not None and self.__current.is_active:
            self.__current.close()

        self.__current = self.get_new_session()
        return self.__current

    def __exit__(self, err, message, traceback):
        if err is None and message is None and traceback is None:
            self.__current.commit()
        else:
            self.__current.rollback()
            current_app._get_current_object().logger.error(
                "Error occured during db session process")

        self.__current.close()


db = Database()
