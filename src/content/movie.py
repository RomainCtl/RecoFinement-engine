from src.utils import db, clean_data, create_soup
import pandas as pd
import numpy as np


class Movie:
    __meta_cols__ = ["user_id", "movie_id",
                     "rating", "watch_count", "review_see_count"]

    id = "movie_id"
    id_type = int
    tablename_recommended = "recommended_movie"
    tablename_similars = "similars_movie"
    tablename_media = "movie"
    uppername = tablename_media.upper()

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
            movie_df["rating_count"] = movie_df["rating_count"].fillna(0)
            movie_df["rating_count"] = movie_df["rating_count"].astype(
                "uint32")
        if "popularity_score" in cols:
            movie_df["popularity_score"] = movie_df["popularity_score"].astype(
                "float32")

        return movie_df

    @classmethod
    def get_meta(cls, cols=None, user_id=None):
        """Get user metamovie metadata

        Returns:
            DataFrame: pandas DataFrame
        """
        if cols is None:
            cols = cls.__meta_cols__
        assert all([x in cls.__meta_cols__ for x in cols])

        filt = ''
        if user_id is not None:
            filt = "WHERE user_id = '%s'" % user_id

        df = pd.read_sql_query('SELECT %s FROM "meta_user_movie" %s' % (
            ', '.join(cols), filt), con=db.engine)

        # Reduce memory usage for ratings
        if 'user_id' in cols:
            df['user_id'] = df['user_id'].astype("uint32")
        if 'movie_id' in cols:
            df['movie_id'] = df['movie_id'].astype("uint16")
        if 'rating' in cols:
            df['rating'] = df['rating'].fillna(0)
            df['rating'] = df['rating'].astype("uint8")
        if 'watch_count' in cols:
            df['watch_count'] = df['watch_count'].astype("uint16")
        if 'review_see_count' in cols:
            df['review_see_count'] = df['review_see_count'].astype("uint16")

        return df

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
    def get_similars(cls, movie_id):
        """Get all similars content of a movie

        Args:
            movie_id (int): movie unique id

        Returns:
            Dataframe: similars movie dataframe
        """
        movie_df = pd.read_sql_query(
            'SELECT sm.movie_id0 AS movie_id, sm.movie_id1 AS similar_movie_id, sm.similarity, m.popularity_score FROM "similars_movie" AS sm INNER JOIN "movie" AS m ON m.movie_id = sm.movie_id1 WHERE movie_id0 = \'%s\'' % movie_id, con=db.engine)

        movie_df = cls.reduce_memory(movie_df)

        return movie_df

    @classmethod
    def get_for_profile(cls):
        movie_df = pd.read_sql_query(
            'SELECT m.movie_id, string_agg(g.content_type || g.name, \',\') AS genres FROM "movie" AS m LEFT OUTER JOIN "movie_genres" AS tg ON tg.movie_id = m.movie_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY m.movie_id', con=db.engine)

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
