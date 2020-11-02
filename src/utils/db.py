from sqlalchemy import create_engine
import os

from settings import DB_URI
from .singleton import Singleton


class DB(Singleton):
    engine = create_engine(DB_URI)
