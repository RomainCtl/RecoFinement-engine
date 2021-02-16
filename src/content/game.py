from src.utils import db, clean_data, create_soup
from .content import Content, ContentType

import pandas as pd
import numpy as np


class Game(Content):
    content_type = ContentType.GAME

    # For similarities between different content (different content type)
    cmp_column_name = "name"
    other_content_cmp = [ContentType.MOVIE, ContentType.SERIE]

    def request_for_popularity(self):
        self.df = pd.read_sql_query(
            'SELECT c.content_id, c.rating, c.rating_count, cc.recommendations FROM "%s" AS c INNER JOIN "%s" AS cc ON cc.content_id = c.content_id' % (self.tablename, str(self.content_type)), con=db.engine)

        self.reduce_memory()

        return self.df

    def calc_popularity_score(self, df):
        # NOTE we do not have any rating for game (cold start), so we use 'recommendations' field instead of 'popularity_score' that is computed by 'reco_engine' service
        df = df.assign(popularity_score=df['recommendations'])

        return df

    def get_with_genres(self):
        """Get game

        NOTE can add 't.rating' and 't.rating_count' column if we introduce popularity filter to content-based engine
            example: this recommender would take the 30 most similar item, calculate the popularity score and then return the top 10

        Returns:
            DataFrame: dataframe of game data
        """
        self.df = pd.read_sql_query(
            'SELECT g.content_id, g.name, g.short_description, g.developers, g.publishers, string_agg(ge.name, \',\') AS genres FROM "%s" AS c INNER JOIN "%s" AS g ON g.content_id = c.content_id LEFT OUTER JOIN "content_genres" AS cg ON cg.content_id = c.content_id LEFT OUTER JOIN "genre" AS ge ON ge.genre_id = cg.genre_id GROUP BY g.content_id' % (self.tablename, self.content_type), con=db.engine)

        # Reduce memory
        self.reduce_memory()

        return self.df

    def prepare_sim(self):
        """Prepare game data for content similarity process

        Returns:
            DataFrame: result dataframe
        """
        game_df = self.get_with_genres()
        game_df["content_type"] = self.content_type
        # Transform genres str to list
        game_df["genres"] = game_df["genres"].apply(
            lambda x: str(x).split(","))

        # Replace NaN with an empty string
        features = ['name', 'short_description',
                    'developers', 'publishers', 'genres']
        for feature in features:
            game_df[feature] = game_df[feature].fillna('')

        # Clean and homogenise data
        for feature in features:
            game_df[feature] = game_df[feature].apply(clean_data)

        # Transform all list to simple str with space sep
        game_df["genres"] = game_df["genres"].apply(' '.join)
        game_df["genres"] = game_df["genres"].replace('none', '')

        # Create a new soup feature
        game_df['soup'] = game_df.apply(
            lambda x: create_soup(x, features), axis=1)

        # Delete unused cols (feature)
        game_df = game_df.drop(features, 1)

        return game_df
