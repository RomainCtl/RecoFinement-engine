from src.content import User, ContentType
from src.utils import db, sc
from .engine import Engine

from datetime import datetime
from sqlalchemy import text
from pyspark.sql import SQLContext
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS

import pandas as pd


class CollaborativeFiltering(Engine):
    __engine_priority__ = 5
    max_nb_elem = 10

    def train(self):
        if self.check_if_necessary() is False:
            return

        for media in self.__media__:
            st_time = datetime.utcnow()
            m = media(logger=self.logger)

            if m.content_type in [
                ContentType.GAME,  # no ratings
                ContentType.SERIE,  # too much ratings
                ContentType.MOVIE  # too much ratings
            ]:
                continue

            sqlContext = SQLContext(sc)

            df = m.get_meta(cols=['user_id', m.id, 'rating'])

            # Convert Pandas DF to PySpark DF
            sparkDF = sqlContext.createDataFrame(df)

            als = ALS(userCol="user_id", itemCol=m.id,
                      ratingCol="rating", coldStartStrategy="drop")
            model = als.fit(sparkDF)

            user_df = User.get()

            # Check if is empty
            if user_df.shape[0] == 0:
                continue

            modelGest = model.recommendForUserSubset(
                sqlContext.createDataFrame(user_df), self.max_nb_elem)

            len_values = 0

            for user in modelGest.collect():
                # Do not recommend already recommended content
                already_recommended_media = []
                with db as session:
                    result = session.execute('SELECT %s FROM "%s" WHERE user_id = \'%s\' AND engine <> \'%s\'' % (
                        m.id, m.tablename_recommended, user.user_id, self.__class__.__name__))
                    already_recommended_media = [
                        dict(row)[m.id] for row in result]

                values = []
                for rating in user.recommendations:
                    id = int(rating[m.id])
                    if id in already_recommended_media:
                        continue
                    values.append(
                        {
                            "user_id": int(user.user_id),
                            m.id: id,
                            # divide by 5 to get a score between 0 and 1
                            "score": float(rating.rating / 5),
                            "engine": self.__class__.__name__,
                            "engine_priority": self.__engine_priority__,
                        }
                    )

                len_values += len(values)

                with db as session:
                    # Reset list of recommended `media` for this engine
                    session.execute(text('DELETE FROM "%s" WHERE user_id = %s AND engine = \'%s\' AND content_type = \'%s\'' % (
                        m.tablename_recommended, user.user_id, self.__class__.__name__, str(m.content_type).upper())))

                    if len(values) > 0:
                        markers = ':user_id, :%s, :score, :engine, :engine_priority' % m.id
                        ins = 'INSERT INTO {tablename} VALUES ({markers})'
                        ins = ins.format(
                            tablename=m.tablename_recommended, markers=markers)
                        session.execute(ins, values)

            self.logger.info("%s recommendation from collaborative filtering performed in %s (%s lines)" % (
                m.content_type, datetime.utcnow()-st_time, len_values))
            self.store_date(m.content_type)

    def check_if_necessary(self):
        df = pd.read_sql_query(
            'SELECT last_launch_date FROM "engine" WHERE engine = \'%s\'' % self.__class__.__name__, con=db.engine)

        if df.shape[0] == 0:
            # means that this engine has never been launched.
            return True

        last_launch_date = df.iloc[0]["last_launch_date"]

        df = pd.read_sql_query(
            'SELECT COUNT(*) FROM "meta_added_event" WHERE occured_at > %s' % last_launch_date +
            'UNION SELECT COUNT(*) FROM "changed_event" WHERE model_name = \'meta_user_content\' AND occured_at > %s' % last_launch_date, con=db.engine)

        if df.shape[0] != 50:
            # New change occured (at least 50, can be changed)
            return True

        return False
