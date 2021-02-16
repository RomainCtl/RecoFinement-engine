from src.utils import db
from src.content import ContentType, Content
from .engine import Engine

from flask import current_app
from sqlalchemy import text
from datetime import datetime
import pandas as pd
import numpy as np


class Popularity(Engine):
    """(Re-)Set popularity score to each media

    The main purpose it to recommend the top items based on popularity score
    """

    def train(self):
        """(Re)load popularity score of each media
        """
        if self.check_if_necessary() is False:
            return
        # open a transaction
        with db as session:
            # Reset popularity score (delete and re-add column for score)
            session.execute(
                text('ALTER TABLE "%s" DROP COLUMN popularity_score' % self.__media__[0].tablename))
            session.execute(
                text('ALTER TABLE "%s" ADD COLUMN popularity_score DOUBLE PRECISION' % self.__media__[0].tablename))
        for media in self.__media__:
            st_time = datetime.utcnow()

            m = media(logger=self.logger)
            q_df = m.get_populars(size=1000)

            # open a transaction
            with db as session:
                # Set new popularity score
                for index, row in q_df.iterrows():
                    session.execute(
                        text("UPDATE %s SET popularity_score = %s WHERE %s = %s" % (m.tablename, row["popularity_score"], m.id, row[m.id])))
            self.logger.debug("%s popularity reloading performed in %s (%s lines)" %
                              (str(m.content_type) or "ALL CONTENT", datetime.utcnow()-st_time, q_df.shape[0]))
            self.store_date(m.content_type)

    def check_if_necessary(self):
        for media in self.__media__:
            df = pd.read_sql_query(
                'SELECT last_launch_date FROM "engine" WHERE engine = \'%s\' AND content_type = \'%s\'' % (self.__class__.__name__, str(media.content_type).upper()), con=db.engine)

            if df.shape[0] == 0:
                # means that this engine has never been launched.
                return True

            last_launch_date = df.iloc[0]["last_launch_date"]

            df = pd.read_sql_query(
                'SELECT COUNT(*) AS c FROM "%s_added_event" WHERE occured_at > \'%s\'' % (media.content_type, last_launch_date), con=db.engine)

            if df.iloc[0]["c"] != 0:
                # New change occured
                return True

        return False
