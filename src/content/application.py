from src.utils import DB
import pandas as pd
import numpy as np


class Application:
    __meta_cols__ = ["user_id", "app_id", "review", "popularity",
                     "subjectivity", "rating", "review_see_count", "downloaded"]

    @staticmethod
    def reduce_memory(app_df):
        cols = list(app_df.columns)

        # Reduce memory
        if "app_id" in cols:
            app_df["app_id"] = app_df["app_id"].astype("uint16")
        if "genre_id" in cols:
            app_df["genre_id"] = app_df["genre_id"].astype("uint8")
        if "rating" in cols:
            app_df["rating"] = app_df["rating"].astype("float32")
        if "reviews" in cols:
            app_df["reviews"] = app_df["reviews"].astype("uint32")
        if "popularity_score" in cols:
            app_df["popularity_score"] = app_df["popularity_score"].astype(
                "float32")

        return app_df

    @classmethod
    def get_meta(cls, cols=None):
        pass

    @classmethod
    def get_ratings(cls):
        """Get all application and their metadata

        Returns:
            DataFrame: application dataframe
        """
        app_df = pd.read_sql_query(
            'SELECT app_id, rating, reviews as rating_count FROM "application"', con=DB.engine)

        # Reduce memory
        app_df = cls.reduce_memory(app_df)

        return app_df

    @classmethod
    def get_applications(cls):
        app_df = pd.read_sql_query(
            'SELECT * FROM "application"', con=DB.engine)

        # Reduce memory
        app_df = cls.reduce_memory(app_df)

        return app_df

    @classmethod
    def get(cls):
        pass

    @staticmethod
    def get_with_genres(app_df):
        pass
