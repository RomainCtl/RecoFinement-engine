from src.utils import DB
import pandas as pd
import numpy as np


class Movie:
    __meta_cols__ = ["user_id", "movie_id",
                     "rating", "watch_count", "review_see_count"]

    @staticmethod
    def reduce_memory(movie_df):
        cols = list(movie_df.columns)

        # Replace all 'unknown' value by nan
        movie_df = movie_df.replace("unknown", np.nan)

        if "year" in cols:
            movie_df["year"] = movie_df["year"].fillna(0)

        # Reduce memory
        if "movie_id" in cols:
            movie_df["movie_id"] = movie_df["movie_id"].astype("uint32")
        if "year" in cols:
            movie_df["year"] = movie_df["year"].astype("uint16")
        if "rating" in cols:
            movie_df["rating"] = movie_df["rating"].astype("float32")
        if "rating_count" in cols:
            movie_df["rating_count"] = movie_df["rating_count"].astype(
                "uint32")
        if "popularity_score" in cols:
            movie_df["popularity_score"] = movie_df["popularity_score"].astype(
                "float32")

        return movie_df

    @classmethod
    def get_meta(cls, cols=None):
        pass

    @classmethod
    def get_ratings(cls):
        """Get all movies and their metadata

        Returns:
            DataFrame: movie dataframe
        """
        movie_df = pd.read_sql_query(
            'SELECT movie_id, rating, rating_count FROM "movie"', con=DB.engine)

        # Reduce memory
        movie_df = cls.reduce_memory(movie_df)

        return movie_df

    @classmethod
    def get_ratings(cls):
        """Get all movies and their metadata

        Returns:
            DataFrame: movie dataframe
        """
        track_df = pd.read_sql_query(
            'SELECT movie_id, rating, rating_count FROM "movie"', con=DB.engine)

        # Reduce memory
        track_df = cls.reduce_memory(track_df)

        return track_df

    @classmethod
    def get_movies(cls):
        movie_df = pd.read_sql_query('SELECT * FROM "movie"', con=DB.engine)

        # Reduce memory
        movie_df = cls.reduce_memory(movie_df)

        return movie_df

    @classmethod
    def get(cls):
        pass

    @staticmethod
    def get_with_genres(movie_df):
        pass
