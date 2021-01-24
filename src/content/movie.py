from src.utils import db, clean_data, create_soup
from .content import Content, ContentType

import pandas as pd
import numpy as np


class Movie(Content):
    content_type = ContentType.MOVIE

    def request_for_popularity(self):
        return super().request_for_popularity(self.content_type)

    @classmethod
    def get_for_profile(cls):
        movie_df = pd.read_sql_query(
            'SELECT m.movie_id, string_agg(g.content_type || g.name, \',\') AS genres FROM "movie" AS m LEFT OUTER JOIN "movie_genres" AS tg ON tg.movie_id = m.movie_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY m.movie_id', con=db.engine)

        # Reduce memory
        movie_df = cls.reduce_memory(movie_df)

        return movie_df

    def get_with_genres(self):
        """Get movie

        NOTE can add 't.rating' and 't.rating_count' column if we introduce popularity filter to content-based engine
            example: this recommender would take the 30 most similar item, calculate the popularity score and then return the top 10

        Returns:
            DataFrame: dataframe of movie data
        """
        self.df = pd.read_sql_query(
            'SELECT t.content_id, t.title, t.language, t.actors, t.year, t.producers, t.director, t.writer, string_agg(ge.name, \',\') AS genres FROM "%s" AS c INNER JOIN "%s" AS t ON t.content_id = c.content_id LEFT OUTER JOIN "content_genres" AS cg ON cg.content_id = c.content_id LEFT OUTER JOIN "genre" AS ge ON ge.genre_id = cg.genre_id GROUP BY t.content_id' % (self.tablename, self.content_type), con=db.engine)

        # Reduce memory
        self.reduce_memory()

        return self.df

    @staticmethod
    def prepare_from_user_profile(movie_df):
        """Get movie with genre

        Args:
            movie_df (DataFrame): movie dataframe

        Returns:
            DataFrame: movie with genre weight (0 or 1)
        """

        # Copying the movie dataframe into a new one since we won't need to use the genre information in our first case.
        movieWithGenres_df = movie_df.copy()

        # For every row in the dataframe, iterate through the list of genres and place a 1 into the corresponding column
        for index, row in movie_df.iterrows():
            if row['genres'] is not None:
                for genre in row['genres'].split(","):
                    movieWithGenres_df.at[index, genre] = 1

        # Filling in the NaN values with 0 to show that a movie doesn't have that column's genre
        movieWithGenres_df = movieWithGenres_df.fillna(0)

        # Reduce memory
        genre_cols = list(set(movieWithGenres_df.columns) -
                          set(movie_df.columns))
        for c in genre_cols:
            movieWithGenres_df[c] = movieWithGenres_df[c].astype("uint8")

        movieWithGenres_df.drop(["genres"], axis=1, inplace=True)

        return movieWithGenres_df

    def prepare_sim(self):
        """Prepare movie data for content similarity process

        Returns:
            DataFrame: result dataframe
        """
        movie_df = self.get_with_genres()
        # Remove '0' from year
        movie_df["year"] = movie_df["year"].astype(str)
        movie_df["year"] = movie_df["year"].replace('0', '')

        # Replace NaN with an empty string
        features = ['title', 'language', 'actors',
                    'producers', 'director', 'writer', 'genres']
        for feature in features:
            movie_df[feature] = movie_df[feature].fillna('')

        # Transform multiple str to list
        # NOTE only take the first 5 feature (due to performence issue, lack of material resource)
        movie_df["genres"] = movie_df["genres"].apply(
            lambda x: str(x).split(","))
        movie_df["actors"] = movie_df["actors"].apply(
            lambda x: str(x).split("|")[:5])
        movie_df["producers"] = movie_df["producers"].apply(
            lambda x: str(x).split("|")[:5])

        # Clean and homogenise data
        for feature in features:
            movie_df[feature] = movie_df[feature].apply(clean_data)

        # Transform all list to simple str with space sep
        movie_df["genres"] = movie_df["genres"].apply(' '.join)
        movie_df["actors"] = movie_df["actors"].apply(' '.join)
        movie_df["producers"] = movie_df["producers"].apply(' '.join)

        # Create a new soup feature
        movie_df['soup'] = movie_df.apply(
            lambda x: create_soup(x, features), axis=1)

        # Delete unused cols (feature)
        features = ['title', 'language', 'actors',
                    'producers', 'director', 'writer', 'genres', 'year']
        movie_df = movie_df.drop(features, 1)

        return movie_df
