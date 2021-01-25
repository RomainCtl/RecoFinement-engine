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
        # open a transaction
        with db as session:
            # Reset popularity score (delete and re-add column for score)
            session.execute(
                text('ALTER TABLE "%s" DROP COLUMN popularity_score' % m.tablename))
            session.execute(
                text('ALTER TABLE "%s" ADD COLUMN popularity_score DOUBLE PRECISION' % m.tablename))
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
