from src.utils import db, sc, parallelize_matrix, broadcast_matrix, find_matches_in_submatrix
from .engine import Engine

from scipy.sparse import csr_matrix

from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sqlalchemy import text
from datetime import datetime
import pandas as pd
import numpy as np


class ContentSimilarities(Engine):
    """(Re-)Set similarity score between to item (for each media)

    The main purpose it to recommend similar items based on a particular item
    """

    def train(self):
        """(Re)load similarity score between item
        """
        for media in self.__media__:
            st_time = datetime.utcnow()
            m = media(logger=self.logger)

            df = m.prepare_sim()[:100_000]
            # FIXME limitation of dataframe (Let's go to bigdata...), we juste need a cluster (currently, we use spark standalone)

            # Pre-calculate voc
            vect = CountVectorizer(stop_words="english")
            # This can be done with less memory overhead by using generator
            vocabulary = vect.fit(df["soup"]).vocabulary_

            tfidf = TfidfVectorizer(
                stop_words="english", vocabulary=vocabulary, dtype=np.float32)

            self.logger.debug("%s data preparation performed in %s" %
                              (m.content_type, datetime.utcnow()-st_time))

            # Construct the required TF-IDF matrix by fitting and transforming the data
            tfidf_matrix = tfidf.fit_transform(df['soup'])

            self.logger.debug("%s TF-IDF transformation performed in %s" %
                              (m.content_type, datetime.utcnow()-st_time))

            # Reset index of your main DataFrame and construct reverse mapping as before
            df = df.reset_index()
            indices = pd.Series(df.index, index=df[m.id])

            tfidf_mat_para = parallelize_matrix(
                tfidf_matrix, rows_per_chunk=100)
            tfidf_mat_dist = broadcast_matrix(tfidf_matrix)

            values = tfidf_mat_para.flatMap(lambda submatrix: find_matches_in_submatrix(
                sources=csr_matrix(submatrix[1], shape=submatrix[2]),
                targets=tfidf_mat_dist,
                inputs_start_index=submatrix[0],
                indices=indices,
                real_indice_type=int,
                real_indice_name=m.id)
            ).collect()

            self.logger.debug("%s cosine sim performed in %s" %
                              (str(m.content_type), datetime.utcnow()-st_time))

            with db as session:
                # Reset popularity score (delete and re-add column for score)
                session.execute(
                    text('TRUNCATE TABLE "%s" RESTART IDENTITY' % m.tablename_similars))

                markers = ':%s0, :%s1, :similarity' % (m.id, m.id)
                ins = 'INSERT INTO {tablename} VALUES ({markers})'
                ins = ins.format(
                    tablename=m.tablename_similars, markers=markers)
                session.execute(ins, values)

            self.logger.info("%s similarity reloading performed in %s (%s lines)" %
                             (m.content_type, datetime.utcnow()-st_time, len(values)))
