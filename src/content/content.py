from src.utils import db, clean_data, create_soup

from flask import current_app
from sqlalchemy import text
from datetime import datetime

import pandas as pd
import numpy as np
import enum


class ContentType(enum.Enum):
    APPLICATION = "application"
    BOOK = "book"
    GAME = "game"
    MOVIE = "movie"
    SERIE = "serie"
    TRACK = "track"

    def __str__(self):
        return self.value


class Content:
    id = "content_id"
    content_type = None

    tablename_recommended = "recommended_content"
    tablename_recommended_for_group = "recommended_content_for_group"
    tablename_similars = "similars_content"
    tablename_meta = "meta_user_content"
    tablename = "content"

    __meta_cols__ = ["user_id", id, "rating", "last_rating_date",
                     "review_see_count", "last_review_see_date", "count", "last_count_increment"]

    def __init__(self, dataframe: pd.DataFrame = None, logger=None):
        self.df = dataframe
        self.logger = logger or current_app.logger

    def reduce_memory(self):
        cols = list(self.df.columns)

        if self.id in cols:
            self.df[self.id] = self.df[self.id].astype("uint32")
        if "rating" in cols:
            self.df["rating"] = self.df["rating"].astype("float32")
        if "rating_count" in cols:
            self.df["rating_count"] = self.df["rating_count"].fillna(0)
            self.df["rating_count"] = self.df["rating_count"].astype("uint32")
        if "popularity_score" in cols:
            self.df["popularity_score"] = self.df["popularity_score"].astype(
                "float32")

    def _reduce_metadata_memory(self, df: pd.DataFrame):
        if 'user_id' in cols:
            df['user_id'] = df['user_id'].astype("uint32")
        if self.id in cols:
            df[self.id] = df[self.id].astype("uint32")
        if 'rating' in cols:
            df['rating'] = df['rating'].fillna(0).astype("uint8")
        if 'review_see_count' in cols:
            df['review_see_count'] = df['review_see_count'].fillna(
                0).astype("uint16")
        if 'count' in cols:
            df['count'] = df['count'].fillna(0).astype("uint16")

        # last_rating_date
        # last_review_see_date
        # last_count_increment

        return df

    def get_meta(self, cols=None, user_id=None, limit=None):
        """Get metadata as Dataframe

        Args:
            cols (list, optional): Columns to select as list of column name (string). Defaults to None.
            user_id (int|string, optional): User identifier. Defaults to None.

        Returns:
            Dataframe: metadata as pandas Dataframe
        """
        if cols is None:
            cols = self.__meta_cols__
        assert all([x in self.__meta_cols__ for x in cols])

        user_filt = ''
        if user_id is not None:
            user_filt = "WHERE user_id = '%s'" % user_id

        limit_filt = ''
        if limit is not None:
            assert limit > 0, "Limit must be greater than 0"
            limit_filt = "LIMIT %s" % limit

        df = pd.read_sql_query('SELECT %s FROM "%s" %s %s' % (
            ', '.join(cols), self.tablename_meta, user_filt, limit_filt), con=db.engine)

        return self._reduce_metadata_memory(df)

    def request_for_popularity(self, content_type=None):
        # Can be overrided by child class
        assert content_type is None or isinstance(
            content_type, ContentType), "content_type must be 'ContentType' instance or None"

        type_filt = ""
        if content_type is not None:
            content_type = str(content_type)
            type_filt = 'INNER JOIN "%s" AS cc ON cc.content_id = c.content_id' % (
                content_type)

        self.df = pd.read_sql_query(
            'SELECT c.content_id, c.rating, c.rating_count FROM "%s" AS c %s' % (self.tablename, type_filt), con=db.engine)

        self.reduce_memory()

        return self.df

    def calc_popularity_score(self, df):
        """IMDB measure of popularity

        Args:
            df (Dataframe): content dataframe

        Returns:
            Dataframe: df with new column for calculated popularity score
        """
        # Can be overrided by child class
        def weighted_rating(x, m, C):
            """Function that computes the weighted rating of each media

            Args:
                x (DataFrame row): current row
                m (int): minimum number of votes.
                C (float): Average ratings of all medias.

            Returns:
                float: weight popularity score
            """
            v = x['rating_count']
            R = x['rating']

            # Calculation based on the IMDB formula
            return float(format((v/(v+m) * R) + (m/(m+v) * C), ".4f"))

        # Get average ratings of all media
        c = df["rating"].mean()

        # Calculate the minimum number of votes required to be in the chart
        m = df["rating_count"].quantile(0.90)

        # Filter out all qualified media into a new DataFrame
        q_df = df.copy().loc[df['rating_count'] >= m]

        # Define a new feature 'popularity_score' and calculate its value with `weighted_rating()`
        q_df['popularity_score'] = q_df.apply(
            lambda x: weighted_rating(x, m, c), axis=1, result_type="reduce")

        return q_df

    def get_populars(self, size=200):
        """Set popularity score for each content

        Args:
            size (int, optional): number of the most popular media returned. Defaults to 200.

        Returns:
            DataFrame: with columns 'content_id', 'popularity_score'
        """

        df = self.request_for_popularity()

        df["rating"] = df["rating"].replace(0, np.nan)
        df = df[df["rating"].notna()]

        q_df = self.calc_popularity_score(df)

        # Sort content based on score calculated above
        q_df = q_df.sort_values('popularity_score', ascending=False)

        q_df = q_df.drop(['rating', 'rating_count'], 1)

        return q_df.head(size)

    def get_with_genres(self):
        raise Exception("'get_with_genres' function must be created")

    def prepare_sim(self):
        raise Exception("'prepare_sim' function must be created")

    # def get_similars(self, content_id, same_type=True):
    #     """Get all similars content of a content

    #     Args:
    #         content_id (int): content unique id

    #     Returns:
    #         Dataframe: similars content dataframe
    #     """
    #     filt = ""
    #     if same_type:
    #         filt = "AND s.content_type0 = s.content_type1"

    #     self.df = pd.read_sql_query(
    #         'SELECT s.content_id0 AS content_id, s.content_id1 AS similar_content_id FROM %s AS s INNER JOIN "%s" AS c ON c.content_id = s.content_id1 WHERE s.content_id0 = "%s"' % (self.tablename_similars, self.tablename, content_id), con=db.engine)
    #     # app_df = pd.read_sql_query(
    #     #     'SELECT sa.app_id0 AS app_id, sa.app_id1 AS similar_app_id, sa.similarity, a.popularity_score FROM "similars_application" AS sa INNER JOIN "application" AS a ON a.app_id = sa.app_id1 WHERE app_id0 = \'%s\'' % app_id, con=db.engine)

    #     self.reduce_memory()

    #     return self.df
