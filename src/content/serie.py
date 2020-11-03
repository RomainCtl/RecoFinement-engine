from src.utils import db
import pandas as pd
import numpy as np


class Serie:
    __meta_cols__ = ["user_id", "serie_id", "rating",
                     "num_watched_episodes", "review_see_count"]

    @staticmethod
    def reduce_memory(serie_df):
        cols = list(serie_df.columns)

        # Reduce memory
        if "serie_id" in cols:
            serie_df["serie_id"] = serie_df["serie_id"].astype("uint32")
        if "start_year" in cols:
            serie_df["start_year"] = serie_df["start_year"].astype("uint16")
        if "end_year" in cols:
            serie_df["end_year"] = serie_df["end_year"].astype("uint16")
        if "rating" in cols:
            serie_df["rating"] = serie_df["rating"].astype("float32")
        if "rating_count" in cols:
            serie_df["rating_count"] = serie_df["rating_count"].astype(
                "uint32")
        if "popularity_score" in cols:
            serie_df["popularity_score"] = serie_df["popularity_score"].astype(
                "float32")

        return serie_df

    @classmethod
    def get_meta(cls, cols=None):
        pass

    @classmethod
    def get_ratings(cls):
        """Get all series and their metadata

        Returns:
            DataFrame: serie dataframe
        """
        serie_df = pd.read_sql_query(
            'SELECT serie_id, rating, rating_count FROM "serie"', con=db.engine)

        # Reduce memory
        serie_df = cls.reduce_memory(serie_df)

        return serie_df

    @classmethod
    def get_series(cls):
        serie_df = pd.read_sql_query('SELECT * FROM "serie"', con=db.engine)

        # Reduce memory
        serie_df = cls.reduce_memory(serie_df)

        return serie_df

    @classmethod
    def get(cls):
        pass

    @staticmethod
    def get_with_genres(serie_df):
        pass
