from src.utils import db, sc

from .engine import Engine

import pandas as pd
from sqlalchemy import text

from pyspark.sql import SQLContext
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS

class CollaborativeFiltering(Engine):

    __engine_priority__ = 5

    def train(self):

        for media in self.__media__ :

            sqlContext = SQLContext(sc)

            df = media.get_ratings()

            sparkDF = sqlContext.createDataFrame(df)

            itemIdName = media.id

            if media.tablename_media == "book" :
                itemIdName = "isbn_id"
                stringIndexer = StringIndexer(inputCol="isbn", outputCol="isbn_id")
                model = stringIndexer.fit(sparkDF)
                sparkDF = model.transform(sparkDF)
            
            als = ALS(userCol="user_id", itemCol=itemIdName, ratingCol="rating", coldStartStrategy="drop")
            model = als.fit(sparkDF)   

            usersAsDF = pd.read_sql_query("SELECT user_id FROM public.user WHERE password_hash!='no_pwd'", con=engine)
            userList = [ [u] for u in usersAsDF['user_id'].tolist() ]

            nbElem = 10

            modelGest = model.recommendForUserSubset(sqlContext.createDataFrame([userList], schema=['user_id']), nbElem)

            # TODO : process datas
            for user in modelGest.collect() :
                userId = user.user_id

                with db as session:
                    session.execute(text('DELETE FROM public.%s WHERE user_id==%s' % (media.tablename_recommended, userId) ))

                    for rating in user.recommendations :
                        markers = '%s,%s,%s,%s,%s' % (userId, rating[itemIdName], rating.rating, "collaborative_filtering", __engine_priority__)
                        ins = 'INSERT INTO {tablename} VALUES ({markers})'
                        ins = ins.format(tablename=media.tablename_recommended, markers=markers)
                        session.execute(ins, values)