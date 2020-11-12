from src.utils import db, clean_data, create_soup

import pandas as pd
import numpy as np


class Application:
    __meta_cols__ = ["user_id", "app_id", "review", "popularity",
                     "subjectivity", "rating", "review_see_count", "downloaded"]

    id = "app_id"
    id_type = int
    tablename_recommended = "recommended_application"
    tablename_similars = "similars_application"
    tablename_media = "application"
    uppername = tablename_media.upper()

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
        if cols is None:
            cols = cls.__meta_cols__
        assert all([x in cls.__meta_cols__ for x in cols])

        df = pd.read_sql_query('SELECT %s FROM "meta_user_application"' % (
            ', '.join(cols)), con=db.engine)

        # Reduce memory usage for ratings
        if 'user_id' in cols:
            df['user_id'] = df['user_id'].astype("uint32")
        if 'track_id' in cols:
            df['app_id'] = df['app_id'].astype("uint16")
        if 'rating' in cols:
            df['rating'] = df['rating'].astype("uint8")
        if 'review_see_count' in cols:
            df['review_see_count'] = df['review_see_count'].astype("uint16")
        if 'popularity' in cols:
            df['popularity'] = df['popularity'].astype("float2")
        if 'subjectivity' in cols:
            df['subjectivity'] = df['subjectivity'].astype("float32")

        return df

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
    def get_for_profile(cls):
        app_df = pd.read_sql_query(
            'SELECT a.app_id, g.content_type || g.name AS genres FROM "application" AS a LEFT OUTER JOIN "genre" AS g ON g.genre_id = a.genre_id', con=db.engine)

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
            'SELECT t.app_id, t.name, t.type, t.content_rating, g.name AS genres FROM "application" AS t LEFT OUTER JOIN "genre" AS g ON g.genre_id = t.genre_id', con=db.engine)

        # Reduce memory
        app_df = cls.reduce_memory(app_df)

        return app_df

    @staticmethod
    def prepare_from_user_profile(app_df):
        """Get app with genre

        Args:
            app_df (DataFrame): Application dataframe

        Returns:
            DataFrame: app with genre weight (0 or 1)
        """
        # Copying the app dataframe into a new one since we won't need to use the genre information in our first case.
        appWithGenres_df = app_df.copy()

        # For every row in the dataframe, iterate through the list of genres and place a 1 into the corresponding column
        for index, row in app_df.iterrows():
            if row['genres'] is not None:
                for genre in row['genres'].split(","):
                    appWithGenres_df.at[index, genre] = 1

        # Filling in the NaN values with 0 to show that a app doesn't have that column's genre
        appWithGenres_df = appWithGenres_df.fillna(0)

        # Reduce memory
        genre_cols = list(set(appWithGenres_df.columns) -
                          set(app_df.columns))
        for c in genre_cols:
            appWithGenres_df[c] = appWithGenres_df[c].astype("uint8")

        appWithGenres_df.drop(["genres"], axis=1, inplace=True)

        return appWithGenres_df

    @staticmethod
    def prepare_sim(app_df):
        """Prepare application data for content similarity process

        Args:
            app_df (DataFrame): Application dataframe

        Returns:
            DataFrame: result dataframe
        """
        # Replace NaN with an empty string
        features = ['name', 'type', 'content_rating', 'genres']
        for feature in features:
            app_df[feature] = app_df[feature].fillna('')

        # Clean and homogenise data
        for feature in features:
            app_df[feature] = app_df[feature].apply(clean_data)

        # Create a new soup feature
        app_df['soup'] = app_df.apply(
            lambda x: create_soup(x, features), axis=1)

        # Delete unused cols (feature)
        app_df = app_df.drop(features, 1)

        return app_df
