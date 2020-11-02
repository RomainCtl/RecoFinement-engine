from src.content import Application, Book, Game, Movie, Serie, Track
from src.utils import DB
from .engine import Engine

from flask import current_app
from sqlalchemy import text
from datetime import datetime
import pandas as pd
import numpy as np


class Popularity(Engine):
    """(Re-)Set popularity score to each media

    The main purpose it to recommend the top items based on popularity score
    """
    __media__ = {
        "application": (Application, "app_id", int),
        "book": (Book, "isbn", str),
        "game": (Game, "game_id", int),
        "movie": (Movie, "movie_id", int),
        "serie": (Serie, "serie_id", int),
        "track": (Track, "track_id", int)
    }

    def load(self):
        """(Re)load popularity score of each media
        """
        for media in self.__media__:
            st_time = datetime.utcnow()

            info = self.__media__[media]

            df = info[0].get_ratings()

            q_df = self._get_populars(df)

            # open a transaction
            with DB.engine.begin() as connection:
                # Reset popularity score (delete and re-add column for score)
                connection.execute(
                    text('ALTER TABLE "%s" DROP COLUMN popularity_score' % media))
                connection.execute(
                    text('ALTER TABLE "%s" ADD COLUMN popularity_score DOUBLE PRECISION' % media))
                # Set new popularity score
                for index, row in q_df.iterrows():
                    id = row[info[1]]
                    if info[2] == str:
                        id = "'%s'" % row[info[1]]
                    connection.execute(
                        text("UPDATE %s SET popularity_score = %s WHERE %s = %s" % (media, row["popularity_score"], info[1], id)))
            self.logger.debug("%s popularity reloading performed in %s (%s lines)" %
                              (media, datetime.utcnow()-st_time, q_df.shape[0]))

    def _weighted_rating(self, x, m, C):
        """Function that computes the weighted rating of each media

        Args:
            x (DataFrame row): current row
            m (int): minimum number of votes.
            C (float): Average ratings of all medias.

        Returns:
            float: weight popularity score
        """
        v = x['rating_count']
        R = x['rating']

        # Calculation based on the IMDB formula
        return float(format((v/(v+m) * R) + (m/(m+v) * C), ".4f"))

    def _get_populars(self, df, size=200):
        """Set popularity score for each media in 'df'

        Args:
            df (DataFrame): Media dataframe.
            size (int, optional): number of the most popular media returned. Defaults to 200.

        Returns:
            DataFrame: with columns 'id of media', 'popularity_score'
        """
        df["rating"] = df["rating"].replace(0, np.nan)
        df = df[df["rating"].notna()]

        # Get average ratings of all media
        c = df["rating"].mean()

        # Calculate the minimum number of votes required to be in the chart
        m = df["rating_count"].quantile(0.90)

        # Filter out all qualified media into a new DataFrame
        q_df = df.copy().loc[df['rating_count'] >= m]

        # Define a new feature 'popularity_score' and calculate its value with `_weighted_rating()`
        q_df['popularity_score'] = q_df.apply(
            lambda x: self._weighted_rating(x, m, c), axis=1, result_type="reduce")

        # Sort movies based on score calculated above
        q_df = q_df.sort_values('popularity_score', ascending=False)

        q_df = q_df.drop(['rating', 'rating_count'], 1)

        return q_df.head(size)
