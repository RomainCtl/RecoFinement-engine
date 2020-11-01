from sqlalchemy import create_engine
from .singleton import Singleton
import os


class DB(Singleton):
    engine = create_engine("postgresql://%s:%s@%s:%s/%s" % (
        os.environ["DB_USER_LOGIN"], os.environ["DB_USER_PASSWORD"], os.environ["DB_HOST"], os.environ["DB_PORT"], os.environ["DB_NAME"]))
