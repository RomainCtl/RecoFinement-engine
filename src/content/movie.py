from src.utils import db, clean_data, create_soup
import pandas as pd
import numpy as np


class Movie:
    __meta_cols__ = ["user_id", "movie_id",
                     "rating", "watch_count", "review_see_count"]

    @staticmethod
    def reduce_memory(movie_df):
        cols = list(movie_df.columns)

        # Replace all 'unknown' value by nan
        movie_df = movie_df.replace("unknown", np.nan)

        if "year" in cols:
            movie_df["year"] = movie_df["year"].fillna(0)

        # Reduce memory
        if "movie_id" in cols:
            movie_df["movie_id"] = movie_df["movie_id"].astype("uint32")
        if "year" in cols:
            movie_df["year"] = movie_df["year"].astype("uint16")
        if "rating" in cols:
            movie_df["rating"] = movie_df["rating"].astype("float32")
        if "rating_count" in cols:
            movie_df["rating_count"] = movie_df["rating_count"].astype(
                "uint32")
        if "popularity_score" in cols:
            movie_df["popularity_score"] = movie_df["popularity_score"].astype(
                "float32")

        return movie_df

    @classmethod
    def get_meta(cls, cols=None):
        pass

    @classmethod
    def get_ratings(cls):
        """Get all movies and their metadata

        Returns:
            DataFrame: movie dataframe
        """
        movie_df = pd.read_sql_query(
            'SELECT movie_id, rating, rating_count FROM "movie"', con=db.engine)

        # Reduce memory
        movie_df = cls.reduce_memory(movie_df)

        return movie_df

    @classmethod
    def get_ratings(cls):
        """Get all movies and their metadata

        Returns:
            DataFrame: movie dataframe
        """
        movie_df = pd.read_sql_query(
            'SELECT movie_id, rating, rating_count FROM "movie"', con=db.engine)

        # Reduce memory
        movie_df = cls.reduce_memory(movie_df)

        return movie_df

    @classmethod
    def get_movies(cls):
        movie_df = pd.read_sql_query('SELECT * FROM "movie"', con=db.engine)

        # Reduce memory
        movie_df = cls.reduce_memory(movie_df)

        return movie_df

    @classmethod
    def get_with_genres(cls):
        """Get movie

        NOTE can add 't.rating' and 't.rating_count' column if we introduce popularity filter to content-based engine
            example: this recommender would take the 30 most similar item, calculate the popularity score and then return the top 10

        Returns:
            DataFrame: dataframe of movie data
        """
        movie_df = pd.read_sql_query(
            'SELECT t.movie_id, t.title, t.language, t.actors, t.year, t.producers, t.director, t.writer, string_agg(g.name, \',\') AS genres FROM "movie" AS t LEFT OUTER JOIN "movie_genres" AS tg ON tg.movie_id = t.movie_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY t.movie_id', con=db.engine)

        # Reduce memory
        movie_df = cls.reduce_memory(movie_df)

        return movie_df

    @staticmethod
    def prepare_sim(movie_df):
        """Prepare movie data for content similarity process

        Args:
            movie_df (DataFrame): movie dataframe

        Returns:
            DataFrame: result dataframe
        """
        # Remove '0' from year
        movie_df["year"] = movie_df["year"].astype(str)
        movie_df["year"] = movie_df["year"].replace('0', '')

        # Replace NaN with an empty string
        features = ['title', 'language', 'actors',
                    'producers', 'director', 'writer', 'genres']
        for feature in features:
            movie_df[feature] = movie_df[feature].fillna('')

        # Transform multiple str to list
        movie_df["genres"] = movie_df["genres"].apply(
            lambda x: str(x).split(","))
        movie_df["actors"] = movie_df["actors"].apply(
            lambda x: str(x).split("|"))
        movie_df["producers"] = movie_df["producers"].apply(
            lambda x: str(x).split("|"))

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
