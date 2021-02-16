from src.utils import db, sc, parallelize_matrix, broadcast_matrix, find_matches_in_submatrix
from src.content import ContentType, Book, Game, Movie, Serie, Track
from .engine import Engine

from scipy.sparse import csr_matrix

from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sqlalchemy import text
from datetime import datetime
import pandas as pd
import numpy as np


class LinkBetweenItems(Engine):

    def train(self):
        if self.check_if_necessary() is False:
            return
        with db as session:
            session.execute(
                text('DELETE FROM "%s" WHERE content_type0 <> content_type1' % (self.__media__[0].tablename_similars)))

        for media in self.__media__:
            m = media(logger=self.logger)

            if m.content_type in [
                ContentType.APPLICATION,
                ContentType.BOOK
            ]:
                continue

            df_base, o_dfs = m.prepare_sim_between_content()

            # Pre-calculate voc
            vect = CountVectorizer(stop_words="english")

            for i in range(len(o_dfs)):
                st_time = datetime.utcnow()
                df = pd.concat([df_base, o_dfs[i]])[:100_000]
                # FIXME limitation of dataframe (Let's go to bigdata...), we juste need a cluster (currently, we use spark standalone)

                # This can be done with less memory overhead by using generator
                vocabulary = vect.fit(df["soup"]).vocabulary_

                tfidf = TfidfVectorizer(
                    stop_words="english", vocabulary=vocabulary, dtype=np.float32)

                self.logger.debug("%s + %s data preparation performed in %s" %
                                  (m.content_type, m.other_content_cmp[i], datetime.utcnow()-st_time))

                # Construct the required TF-IDF matrix by fitting and transforming the data
                tfidf_matrix = tfidf.fit_transform(df['soup'])

                self.logger.debug("%s + %s TF-IDF transformation performed in %s" %
                                  (m.content_type, m.other_content_cmp[i], datetime.utcnow()-st_time))

                # Reset index of your main DataFrame and construct reverse mapping as before
                df = df.reset_index()
                indices = pd.Series(
                    df.index,
                    index=zip(df[m.id], df["content_type"])
                )

                tfidf_mat_para = parallelize_matrix(
                    tfidf_matrix, rows_per_chunk=100)
                tfidf_mat_dist = broadcast_matrix(tfidf_matrix)

                values = tfidf_mat_para.flatMap(lambda submatrix: find_matches_in_submatrix(
                    sources=csr_matrix(submatrix[1], shape=submatrix[2]),
                    targets=tfidf_mat_dist,
                    inputs_start_index=submatrix[0],
                    indices=indices,
                    real_indice_name=m.id,
                    content_type=(m.content_type, m.other_content_cmp[i]),
                    threshold=.8,
                    max_sim=25)
                ).collect()

                self.logger.debug("%s + %s cosine sim performed in %s" %
                                  (m.content_type, m.other_content_cmp[i], datetime.utcnow()-st_time))

                if len(values) > 0:
                    with db as session:
                        markers = ':%s0, :%s1, :similarity, :content_type0, :content_type1' % (
                            m.id, m.id)
                        ins = 'INSERT INTO {tablename} VALUES ({markers})'
                        ins = ins.format(
                            tablename=m.tablename_similars, markers=markers)
                        session.execute(ins, values)

                self.logger.info("%s + %s similarity reloading performed in %s (%s lines)" %
                                 (m.content_type, m.other_content_cmp[i], datetime.utcnow()-st_time, len(values)))
            self.store_date(m.content_type)

    def check_if_necessary(self):
        for media in self.__media__:
            if media.content_type in [
                ContentType.APPLICATION,
                ContentType.BOOK
            ]:
                continue

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
