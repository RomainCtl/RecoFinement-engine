from src.utils import db, clean_data, create_soup
from .content import Content, ContentType

import pandas as pd
import numpy as np


class Game(Content):
    content_type = ContentType.GAME

    def request_for_popularity(self):
        self.df = pd.read_sql_query(
            'SELECT c.content_id, c.rating, c.rating_count, cc.recommendations FROM "%s" AS c INNER JOIN "%s" AS cc ON cc.content_id = c.content_id' % (self.tablename, str(self.content_type)), con=db.engine)

        self.reduce_memory()

        return self.df

    def calc_popularity_score(self, df):
        # NOTE we do not have any rating for game (cold start), so we use 'recommendations' field instead of 'popularity_score' that is computed by 'reco_engine' service
        df['popularity_score'] = df['recommendations']

        return df

    @classmethod
    def get_similars(cls, game_id):
        """Get all similars content of a game

        Args:
            game_id (int): game unique id

        Returns:
            Dataframe: similars game dataframe
        """
        game_df = pd.read_sql_query(
            'SELECT sg.game_id0 as game_id, sg.game_id1 as similar_game_id, sg.similarity, g.popularity_score FROM "similars_game" AS sg INNER JOIN "game" AS g ON g.game_id = sg.game_id1 WHERE game_id0 = \'%s\'' % game_id, con=db.engine)

        game_df = cls.reduce_memory(game_df)

        return game_df

    @classmethod
    def get_for_profile(cls):
        game_df = pd.read_sql_query(
            'SELECT ga.game_id, string_agg(g.content_type || g.name, \',\') AS genres FROM "game" AS ga LEFT OUTER JOIN "game_genres" AS tg ON tg.game_id = ga.game_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY ga.game_id', con=db.engine)

        # Reduce memory
        game_df = cls.reduce_memory(game_df)

        return game_df

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

    def prepare_sim(self):
        """Prepare game data for content similarity process

        Returns:
            DataFrame: result dataframe
        """
        game_df = self.get_with_genres()
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
