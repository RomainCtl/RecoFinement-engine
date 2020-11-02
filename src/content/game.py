from src.utils import DB
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
            'SELECT game_id, rating, rating_count FROM "game"', con=DB.engine)

        # Reduce memory
        game_df = cls.reduce_memory(game_df)

        return game_df

    @classmethod
    def get_games(cls):
        game_df = pd.read_sql_query('SELECT * FROM "game"', con=DB.engine)

        # Reduce memory
        game_df = cls.reduce_memory(game_df)

        return game_df

    @classmethod
    def get(cls):
        pass

    @staticmethod
    def get_with_genres(game_df):
        pass
