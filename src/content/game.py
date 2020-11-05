from src.utils import db, clean_data, create_soup
import pandas as pd
import numpy as np


class Game:
    __meta_cols__ = ["user_id", "game_id", "purchase",
                     "hours", "rating", "review_see_count"]

    @staticmethod
    def reduce_memory(game_df):
        cols = list(game_df.columns)

        # Reduce memory
        if "game_id" in cols:
            game_df["game_id"] = game_df["game_id"].astype("uint16")
        if "rating" in cols:
            game_df["rating"] = game_df["rating"].astype("float32")
        if "rating_count" in cols:
            game_df["rating_count"] = game_df["rating_count"].astype(
                "uint32")
        if "popularity_score" in cols:
            game_df["popularity_score"] = game_df["popularity_score"].astype(
                "float32")
        if "recommendations" in cols:
            game_df["recommendations"] = game_df["recommendations"].astype(
                "uint32")
        if "steamid" in cols:
            game_df["steamid"] = game_df["steamid"].astype(
                "uint32")

        return game_df

    @classmethod
    def get_meta(cls, cols=None):
        pass

    @classmethod
    def get_ratings(cls):
        """Get all games and their metadata

        Returns:
            DataFrame: game dataframe
        """
        game_df = pd.read_sql_query(
            'SELECT game_id, rating, rating_count FROM "game"', con=db.engine)

        # Reduce memory
        game_df = cls.reduce_memory(game_df)

        return game_df

    @classmethod
    def get_games(cls):
        game_df = pd.read_sql_query('SELECT * FROM "game"', con=db.engine)

        # Reduce memory
        game_df = cls.reduce_memory(game_df)

        return game_df

    @classmethod
    def get_with_genres(cls):
        """Get game

        NOTE can add 't.rating' and 't.rating_count' column if we introduce popularity filter to content-based engine
            example: this recommender would take the 30 most similar item, calculate the popularity score and then return the top 10

        Returns:
            DataFrame: dataframe of game data
        """
        game_df = pd.read_sql_query(
            'SELECT t.game_id, t.name, t.short_description, t.developers, t.publishers, string_agg(g.name, \',\') AS genres FROM "game" AS t LEFT OUTER JOIN "game_genres" AS tg ON tg.game_id = t.game_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY t.game_id', con=db.engine)

        # Reduce memory
        game_df = cls.reduce_memory(game_df)

        return game_df

    @staticmethod
    def prepare_sim(game_df):
        """Prepare game data for content similarity process

        Args:
            game_df (DataFrame): game dataframe

        Returns:
            DataFrame: result dataframe
        """
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
