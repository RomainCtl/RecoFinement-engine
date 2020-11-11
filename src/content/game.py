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
    def get_meta(cls, cols=None, user_id=None):
        if cols is None:
            cols = cls.__meta_cols__
        assert all([x in cls.__meta_cols__ for x in cols])

        filt = ''
        if user_id is not None:
            filt = "WHERE user_id = '%s'" % user_id

        df = pd.read_sql_query('SELECT %s FROM "meta_user_game" %s' % (
            ', '.join(cols), filt), con=db.engine)

        # Reduce memory usage for ratings
        if 'user_id' in cols:
            df['user_id'] = df['user_id'].astype("uint32")
        if 'game_id' in cols:
            df['game_id'] = df['game_id'].astype("uint16")
        if 'rating' in cols:
            df['rating'] = df['rating'].fillna(0)
            df['rating'] = df['rating'].astype("uint8")
        if 'hours' in cols:
            df['hours'] = df['hours'].astype("uint16")
        if 'review_see_count' in cols:
            df['review_see_count'] = df['review_see_count'].astype("uint16")

        return df

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
    def get_for_profile(cls):
        game_df = pd.read_sql_query(
            'SELECT ga.game_id, string_agg(g.content_type || g.name, \',\') AS genres FROM "game" AS ga LEFT OUTER JOIN "game_genres" AS tg ON tg.game_id = ga.game_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY ga.game_id', con=db.engine)

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
    def prepare_from_user_profile(game_df):
        """Get game with genre

        Args:
            game_df (DataFrame): Game dataframe

        Returns:
            DataFrame: game with genre weight (0 or 1)
        """

        # Copying the game dataframe into a new one since we won't need to use the genre information in our first case.
        gameWithGenres_df = game_df.copy()

        # For every row in the dataframe, iterate through the list of genres and place a 1 into the corresponding column
        for index, row in game_df.iterrows():
            if row['genres'] is not None:
                for genre in row['genres'].split(","):
                    gameWithGenres_df.at[index, genre] = 1

        # Filling in the NaN values with 0 to show that a game doesn't have that column's genre
        gameWithGenres_df = gameWithGenres_df.fillna(0)

        # Reduce memory
        genre_cols = list(set(gameWithGenres_df.columns) -
                          set(game_df.columns))
        for c in genre_cols:
            gameWithGenres_df[c] = gameWithGenres_df[c].astype("uint8")

        gameWithGenres_df.drop(["genres"], axis=1, inplace=True)

        return gameWithGenres_df

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
