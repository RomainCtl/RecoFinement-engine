from src.content import User, Game, Serie, Movie
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
        for media in self.__media__:
            st_time = datetime.utcnow()

            if media.tablename_media in [
                Game.tablename_media,  # no ratings
                Serie.tablename_media,  # too much ratings
                Movie.tablename_media  # too much ratings
            ]:
                continue

            sqlContext = SQLContext(sc)

            df = media.get_meta(cols=['user_id', media.id, 'rating'])

            # Convert Pandas DF to PySpark DF
            sparkDF = sqlContext.createDataFrame(df)

            itemIdName = media.id

            # Create numerique id for each book string id
            if media.tablename_media == "book":
                itemIdName = "isbn_id"
                stringIndexer = StringIndexer(
                    inputCol="isbn", outputCol="isbn_id")
                stringIndexerModel = stringIndexer.fit(sparkDF)
                sparkDF = stringIndexerModel.transform(sparkDF)

            als = ALS(userCol="user_id", itemCol=itemIdName,
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
                values = []
                for rating in user.recommendations:
                    values.append(
                        {
                            "user_id": int(user.user_id),
                            media.id: media.id_type(stringIndexerModel.labels[int(rating[itemIdName])] if media.tablename_media == "book" else rating[itemIdName]),
                            # divide by 5 to get a score between 0 and 1
                            "score": float(rating.rating / 5),
                            "engine": self.__class__.__name__,
                            "engine_priority": self.__engine_priority__,
                        }
                    )

                len_values += len(values)

                with db as session:
                    # Reset list of recommended `media` for this engine
                    session.execute(text('DELETE FROM "%s" WHERE user_id = %s AND engine = \'%s\'' % (
                        media.tablename_recommended, user.user_id, self.__class__.__name__)))

                    markers = ':user_id, :%s, :score, :engine, :engine_priority' % media.id
                    ins = 'INSERT INTO {tablename} VALUES ({markers})'
                    ins = ins.format(
                        tablename=media.tablename_recommended, markers=markers)
                    session.execute(ins, values)

            self.logger.info("%s recommendation from collaborative filtering performed in %s (%s lines)" % (
                media.uppername, datetime.utcnow()-st_time, len_values))
