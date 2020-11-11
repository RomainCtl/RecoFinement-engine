from src.content import Application, Book, Game, Movie, Serie, Track
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
    __media__ = {
        "application": (Application, "app_id", int, "similars_application", 100),
        "book": (Book, "isbn", str, "similars_book", 30),
        "game": (Game, "game_id", int, "similars_game", 100),
        "movie": (Movie, "movie_id", int, "similars_movie", 100),
        "serie": (Serie, "serie_id", int, "similars_serie", 100),
        "track": (Track, "track_id", int, "similars_track", 100),
    }

    def train(self):
        """(Re)load similarity score between item
        """
        for media in self.__media__:
            st_time = datetime.utcnow()

            info = self.__media__[media]

            df = info[0].prepare_sim(info[0].get_with_genres())[:100_000]
            # FIXME limitation of dataframe (Let's go to bigdata...), we juste need a cluster (currently, we use spark standalone)

            # Pre-calculate voc
            vect = CountVectorizer(stop_words="english")
            # This can be done with less memory overhead by using generator
            vocabulary = vect.fit(df["soup"]).vocabulary_

            tfidf = TfidfVectorizer(
                stop_words="english", vocabulary=vocabulary, dtype=np.float32)

            self.logger.debug("%s data preparation performed in %s" %
                              (media, datetime.utcnow()-st_time))

            # Construct the required TF-IDF matrix by fitting and transforming the data
            tfidf_matrix = tfidf.fit_transform(df['soup'])

            self.logger.debug("%s TF-IDF transformation performed in %s" %
                              (media, datetime.utcnow()-st_time))

            # Reset index of your main DataFrame and construct reverse mapping as before
            df = df.reset_index()
            indices = pd.Series(df.index, index=df[info[1]])

            tfidf_mat_para = parallelize_matrix(
                tfidf_matrix, rows_per_chunk=info[4])
            tfidf_mat_dist = broadcast_matrix(tfidf_matrix)

            values = tfidf_mat_para.flatMap(lambda submatrix: find_matches_in_submatrix(
                sources=csr_matrix(submatrix[1], shape=submatrix[2]),
                targets=tfidf_mat_dist,
                inputs_start_index=submatrix[0],
                indices=indices,
                real_indice_type=info[2],
                real_indice_name=info[1])
            ).collect()

            self.logger.debug("%s cosine sim performed in %s" %
                              (media, datetime.utcnow()-st_time))

            with db as session:
                # Reset popularity score (delete and re-add column for score)
                session.execute(
                    text('TRUNCATE TABLE "%s" RESTART IDENTITY' % info[3]))

                markers = ':%s0, :%s1, :similarity' % (info[1], info[1])
                ins = 'INSERT INTO {tablename} VALUES ({markers})'
                ins = ins.format(tablename=info[3], markers=markers)
                session.execute(ins, values)

            self.logger.info("%s similarity reloading performed in %s (%s lines)" %
                             (media, datetime.utcnow()-st_time, len(values)))

    def get_recommendations(self, item_id, cosine_sim):
        """Function that takes item id as input and outputs most similar items

        Args:
            item_id (int|str): item identifier (commonly int, but can be string).
            cosine_sim (DataFrame): DataFrame that store cosine similarities between two iem.

        Returns:
            list: top 10 most similars items (list of tuple item_indice, similarity_score)
        """
        # Get the pairwsie similarity scores of all items with that item
        sim_scores = list(enumerate(cosine_sim[item_id]))

        # Sort the items based on the similarity scores
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

        # Get the scores of the 10 most similar items
        sim_scores = sim_scores[1:11]

        # Delete similar item with score minus than 0.1
        s = None
        for i in range(len(sim_scores)):
            if sim_scores[i][1] < 0.1:
                s = i
                break
        if s is not None:
            del sim_scores[s:11]

        # Get the item indices
        return sim_scores
