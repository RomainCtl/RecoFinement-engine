from src.utils import db, clean_data, create_soup
import pandas as pd
import numpy as np


class Serie:
    __meta_cols__ = ["user_id", "serie_id", "rating",
                     "num_watched_episodes", "review_see_count"]

    @staticmethod
    def reduce_memory(serie_df):
        cols = list(serie_df.columns)

        # Reduce memory
        if "serie_id" in cols:
            serie_df["serie_id"] = serie_df["serie_id"].astype("uint32")
        if "start_year" in cols:
            serie_df["start_year"] = serie_df["start_year"].replace(np.nan, 0)
            serie_df["start_year"] = serie_df["start_year"].astype("uint16")
        if "end_year" in cols:
            serie_df["end_year"] = serie_df["end_year"].replace(np.nan, 0)
            serie_df["end_year"] = serie_df["end_year"].astype("uint16")
        if "rating" in cols:
            serie_df["rating"] = serie_df["rating"].astype("float32")
        if "rating_count" in cols:
            serie_df["rating_count"] = serie_df["rating_count"].astype(
                "uint32")
        if "popularity_score" in cols:
            serie_df["popularity_score"] = serie_df["popularity_score"].astype(
                "float32")

        return serie_df

    @classmethod
    def get_meta(cls, cols=None, user_id=None):
        """Get user metaserie metadata

        Returns:
            DataFrame: pandas DataFrame
        """
        if cols is None:
            cols = cls.__meta_cols__
        assert all([x in cls.__meta_cols__ for x in cols])

        filt = ''
        if user_id is not None:
            filt = "WHERE user_id = '%s'" % user_id

        df = pd.read_sql_query('SELECT %s FROM "meta_user_serie" %s' % (
            ', '.join(cols), filt), con=db.engine)

        # Reduce memory usage for ratings
        if 'user_id' in cols:
            df['user_id'] = df['user_id'].astype("uint32")
        if 'serie_id' in cols:
            df['serie_id'] = df['serie_id'].astype("uint16")
        if 'rating' in cols:
            df['rating'] = df['rating'].fillna(0)
            df['rating'] = df['rating'].astype("uint8")
        if 'num_watched_episodes' in cols:
            df['num_watched_episodes'] = df['num_watched_episodes'].astype(
                "uint16")
        if 'review_see_count' in cols:
            df['review_see_count'] = df['review_see_count'].astype("uint16")

        return df

    @classmethod
    def get_ratings(cls):
        """Get all series and their metadata

        Returns:
            DataFrame: serie dataframe
        """
        serie_df = pd.read_sql_query(
            'SELECT serie_id, rating, rating_count FROM "serie"', con=db.engine)

        # Reduce memory
        serie_df = cls.reduce_memory(serie_df)

        return serie_df

    @classmethod
    def get_for_profile(cls):
        serie_df = pd.read_sql_query(
            'SELECT s.serie_id, string_agg(g.content_type || g.name, \',\') AS genres FROM "serie" AS s LEFT OUTER JOIN "serie_genres" AS tg ON tg.serie_id = s.serie_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY s.serie_id', con=db.engine)

        # Reduce memory
        serie_df = cls.reduce_memory(serie_df)

        return serie_df

    @classmethod
    def get_with_genres(cls):
        """Get serie

        NOTE can add 't.rating' and 't.rating_count' column if we introduce popularity filter to content-based engine
            example: this recommender would take the 30 most similar item, calculate the popularity score and then return the top 10

        Returns:
            DataFrame: dataframe of serie data
        """
        serie_df = pd.read_sql_query(
            'SELECT t.serie_id, t.title, t.start_year, t.writers, t.directors, t.actors, string_agg(g.name, \',\') AS genres FROM "serie" AS t LEFT OUTER JOIN "serie_genres" AS tg ON tg.serie_id = t.serie_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY t.serie_id', con=db.engine)

        # Reduce memory
        serie_df = cls.reduce_memory(serie_df)

        return serie_df

    @staticmethod
    def prepare_from_user_profile(serie_df):
        """Get serie with genre

        Args:
            serie_df (DataFrame): serie dataframe

        Returns:
            DataFrame: serie with genre weight (0 or 1)
        """

        # Copying the serie dataframe into a new one since we won't need to use the genre information in our first case.
        serieWithGenres_df = serie_df.copy()

        # For every row in the dataframe, iterate through the list of genres and place a 1 into the corresponding column
        for index, row in serie_df.iterrows():
            if row['genres'] is not None:
                for genre in row['genres'].split(","):
                    serieWithGenres_df.at[index, genre] = 1

        # Filling in the NaN values with 0 to show that a serie doesn't have that column's genre
        serieWithGenres_df = serieWithGenres_df.fillna(0)

        # Reduce memory
        genre_cols = list(set(serieWithGenres_df.columns) -
                          set(serie_df.columns))
        for c in genre_cols:
            serieWithGenres_df[c] = serieWithGenres_df[c].astype("uint8")

        serieWithGenres_df.drop(["genres"], axis=1, inplace=True)

        return serieWithGenres_df

    @staticmethod
    def prepare_sim(serie_df):
        """Prepare serie data for content similarity process

        Args:
            serie_df (DataFrame): serie dataframe

        Returns:
            DataFrame: result dataframe
        """
        # Remove '0' from year
        serie_df["start_year"] = serie_df["start_year"].astype(str)
        serie_df["start_year"] = serie_df["start_year"].replace('0', '')

        # Replace NaN with an empty string
        features = ["title", "writers", "directors", "actors", "genres"]
        for feature in features:
            serie_df[feature] = serie_df[feature].fillna('')

        # Transform multiple str to list
        # NOTE only take the first 5 feature (due to performence issue, lack of material resource)
        serie_df["genres"] = serie_df["genres"].apply(
            lambda x: str(x).split(","))
        serie_df["writers"] = serie_df["writers"].apply(
            lambda x: str(x).split(",")[:5])
        serie_df["directors"] = serie_df["directors"].apply(
            lambda x: str(x).split(",")[:5])
        serie_df["actors"] = serie_df["actors"].apply(
            lambda x: str(x).split(",")[:5])

        # Clean and homogenise data
        for feature in features:
            serie_df[feature] = serie_df[feature].apply(clean_data)

        # Transform all list to simple str with space sep
        serie_df["genres"] = serie_df["genres"].apply(' '.join)
        serie_df["writers"] = serie_df["writers"].apply(' '.join)
        serie_df["directors"] = serie_df["directors"].apply(' '.join)
        serie_df["actors"] = serie_df["actors"].apply(' '.join)

        # Create a new soup feature
        serie_df['soup'] = serie_df.apply(
            lambda x: create_soup(x, features), axis=1)

        # Delete unused cols (feature)
        features = ["title", "writers", "directors",
                    "actors", "genres", "start_year"]
        serie_df = serie_df.drop(features, 1)

        return serie_df
