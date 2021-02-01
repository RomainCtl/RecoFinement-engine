from src.utils import db, clean_data, create_soup

from flask import current_app
from sqlalchemy import text
from datetime import datetime

import pandas as pd
import numpy as np
import enum
import importlib


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

    # For similarities between different content (different content type)
    cmp_column_name = None
    other_content_cmp = []

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
        cols = list(df.columns)

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

        df = pd.read_sql_query('SELECT %s FROM "%s" AS m INNER JOIN "%s" AS c ON c.content_id = m.content_id INNER JOIN "%s" AS ct ON ct.content_id = c.content_id %s %s' % (
            'm.'+', m.'.join(cols), self.tablename_meta, self.tablename, self.content_type, user_filt, limit_filt), con=db.engine)

        return self._reduce_metadata_memory(df)

    def request_for_popularity(self, content_type=None):
        # Can be overrided by child class
        assert content_type is not None and isinstance(
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

    def get_similars(self, content_id, same_type=True):
        """Get all similars content of a content

        Args:
            content_id (int): content unique id
            same_type (bool, optional): Select content with the same type if True. Defaults to True.

        Returns:
            Dataframe: similars content dataframe
        """
        filt = ""
        if same_type:
            filt = "AND s.content_type0 = s.content_type1"

        self.df = pd.read_sql_query(
            'SELECT s.content_id0 AS content_id, s.content_id1 AS similar_content_id FROM %s AS s INNER JOIN "%s" AS c ON c.content_id = s.content_id1 WHERE s.content_id0 = \'%s\' %s' % (self.tablename_similars, self.tablename, content_id, filt), con=db.engine)

        self.reduce_memory()

        return self.df

    def get_for_profile(self):
        self.df = pd.read_sql_query(
            'SELECT ga.content_id, string_agg(g.content_type || g.name, \',\') AS genres FROM "%s" AS ga INNER JOIN "%s" AS cc ON cc.content_id = ga.content_id LEFT OUTER JOIN "content_genres" AS tg ON tg.content_id = ga.content_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY ga.content_id' % (self.tablename, self.content_type), con=db.engine)

        # Reduce memory
        self.reduce_memory()

        return self.df

    def prepare_from_user_profile(self, df):
        """Get content with genre

        Args:
            df (DataFrame): Content dataframe

        Returns:
            DataFrame: content with genre weight (0 or 1)
        """

        # Copying the content dataframe into a new one since we won't need to use the genre information in our first case.
        contentWithGenres_df = df.copy()

        # For every row in the dataframe, iterate through the list of genres and place a 1 into the corresponding column
        for index, row in df.iterrows():
            if row['genres'] is not None:
                for genre in row['genres'].split(","):
                    contentWithGenres_df.at[index, genre] = 1

        # Filling in the NaN values with 0 to show that a content doesn't have that column's genre
        contentWithGenres_df = contentWithGenres_df.fillna(0)

        # Reduce memory
        genre_cols = list(set(contentWithGenres_df.columns) -
                          set(df.columns))
        for c in genre_cols:
            contentWithGenres_df[c] = contentWithGenres_df[c].astype("uint8")

        contentWithGenres_df.drop(["genres"], axis=1, inplace=True)

        return contentWithGenres_df

    def prepare_sim_between_content(self):
        dfs = []
        content_module = importlib.import_module("src.content")
        frames = [
            self,
            *[
                getattr(content_module, str(o).capitalize())
                for o in self.other_content_cmp
            ]
        ]
        for content in frames:
            tmp = pd.read_sql_query(
                'SELECT cc.content_id, cc.%s AS name FROM "%s" AS c INNER JOIN "%s" AS cc ON cc.content_id = c.content_id GROUP BY cc.content_id' % (content.cmp_column_name, self.tablename, content.content_type), con=db.engine)
            tmp["content_type"] = str(content.content_type)
            dfs.append(tmp)

        for i in range(len(dfs)):
            # Replace NaN with empty string
            dfs[i]["name"].fillna('', inplace=True)

            # Clean and homogenise data
            dfs[i]["name"] = dfs[i]["name"].apply(clean_data)

            # Create a soup
            dfs[i]['soup'] = dfs[i].apply(
                lambda x: create_soup(x, ["name"]), axis=1)

            # Delete unused col
            dfs[i] = dfs[i].drop(["name"], 1)

        return dfs[0], dfs[1:]
