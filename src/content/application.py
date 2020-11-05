from src.utils import db, clean_data, create_soup

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
            'SELECT app_id, rating, reviews as rating_count FROM "application"', con=db.engine)

        # Reduce memory
        app_df = cls.reduce_memory(app_df)

        return app_df

    @classmethod
    def get_applications(cls):
        app_df = pd.read_sql_query(
            'SELECT * FROM "application"', con=db.engine)

        # Reduce memory
        app_df = cls.reduce_memory(app_df)

        return app_df

    @classmethod
    def get_with_genres(cls):
        """Get application

        NOTE can add 't.rating' and 't.reviews as rating_count' column if we introduce popularity filter to content-based engine
            example: this recommender would take the 30 most similar item, calculate the popularity score and then return the top 10

        Returns:
            DataFrame: dataframe of application data
        """
        app_df = pd.read_sql_query(
            'SELECT t.app_id, t.name, t.type, t.content_rating, g.name AS genre FROM "application" AS t LEFT OUTER JOIN "genre" AS g ON g.genre_id = t.genre_id', con=db.engine)

        # Reduce memory
        app_df = cls.reduce_memory(app_df)

        return app_df

    @staticmethod
    def prepare_sim(app_df):
        """Prepare application data for content similarity process

        Args:
            app_df (DataFrame): Application dataframe

        Returns:
            DataFrame: result dataframe
        """
        # Replace NaN with an empty string
        features = ['name', 'type', 'content_rating', 'genre']
        for feature in features:
            app_df[feature] = app_df[feature].fillna('')

        # Parse the stringified features into their corresponding python objects
        # from ast import literal_eval
        # for feature in features:
        #     app_df[feature] = app_df[feature].apply(literal_eval)

        # Clean and homogenise data
        for feature in features:
            app_df[feature] = app_df[feature].apply(clean_data)

        # Create a new soup feature
        app_df['soup'] = app_df.apply(
            lambda x: create_soup(x, features), axis=1)

        # Delete unused cols (feature)
        app_df = app_df.drop(features, 1)

        return app_df
