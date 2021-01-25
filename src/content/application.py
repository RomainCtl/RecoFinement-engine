from src.utils import db, clean_data, create_soup
from .content import Content, ContentType

import pandas as pd
import numpy as np


class Application(Content):
    content_type = ContentType.APPLICATION

    # For similarities between different content (different content type)
    cmp_column_name = "name"

    def request_for_popularity(self):
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

    def get_with_genres(self):
        """Get application

        NOTE can add 't.rating' and 't.reviews as rating_count' column if we introduce popularity filter to content-based engine
            example: this recommender would take the 30 most similar item, calculate the popularity score and then return the top 10

        Returns:
            DataFrame: dataframe of application data
        """
        self.df = pd.read_sql_query(
            'SELECT c.content_id, t.name, t.type, t.content_rating, ge.name AS genres FROM "%s" AS c INNER JOIN "%s" AS t ON t.content_id = c.content_id LEFT OUTER JOIN "content_genres" AS cg ON cg.content_id = c.content_id LEFT OUTER JOIN "genre" AS ge ON ge.genre_id = cg.genre_id' % (self.tablename, self.content_type), con=db.engine)

        # Reduce memory
        self.reduce_memory()

        return self.df

    def prepare_sim(self):
        """Prepare application data for content similarity process

        Returns:
            DataFrame: result dataframe
        """
        app_df = self.get_with_genres()
        app_df["content_type"] = self.content_type
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
