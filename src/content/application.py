from src.utils import db, clean_data, create_soup
from .content import Content, ContentType

import pandas as pd
import numpy as np


class Application(Content):
    content_type = ContentType.APPLICATION

    def request_for_popularity(self):
        print("CHILD %s" % self.content_type)
        return super().request_for_popularity(self.content_type)

    def calc_popularity_score(self, df):
        # NOTE IMDB measure of popularity does not seem to be relevant for this media.

        # Calculate the minimum number of votes required to be in the chart
        m = df["rating_count"].quantile(0.90)

        # Filter out all qualified media into a new DataFrame
        q_df = df.copy().loc[df['rating_count'] >= m]

        q_df['popularity_score'] = q_df.apply(
            lambda x: float(format(x["rating_count"] + x["rating"], ".4f")), axis=1, result_type="reduce")

        return q_df

    @classmethod
    def get_similars(cls, app_id):
        """Get all similars content of an app

        Args:
            app_id (int): app unique id

        Returns:
            Dataframe: similars application dataframe
        """
        app_df = pd.read_sql_query(
            'SELECT sa.app_id0 AS app_id, sa.app_id1 AS similar_app_id, sa.similarity, a.popularity_score FROM "similars_application" AS sa INNER JOIN "application" AS a ON a.app_id = sa.app_id1 WHERE app_id0 = \'%s\'' % app_id, con=db.engine)

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
