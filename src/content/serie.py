from src.utils import db, clean_data, create_soup
from .content import Content, ContentType

import pandas as pd
import numpy as np


class Serie(Content):
    content_type = ContentType.SERIE

    def request_for_popularity(self):
        return super().request_for_popularity(self.content_type)

    @classmethod
    def get_for_profile(cls):
        serie_df = pd.read_sql_query(
            'SELECT s.serie_id, string_agg(g.content_type || g.name, \',\') AS genres FROM "serie" AS s LEFT OUTER JOIN "serie_genres" AS tg ON tg.serie_id = s.serie_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY s.serie_id', con=db.engine)

        # Reduce memory
        serie_df = cls.reduce_memory(serie_df)

        return serie_df

    def get_with_genres(self):
        """Get serie

        NOTE can add 't.rating' and 't.rating_count' column if we introduce popularity filter to content-based engine
            example: this recommender would take the 30 most similar item, calculate the popularity score and then return the top 10

        Returns:
            DataFrame: dataframe of serie data
        """
        self.df = pd.read_sql_query(
            'SELECT t.content_id, t.title, t.start_year, t.writers, t.directors, t.actors, string_agg(ge.name, \',\') AS genres FROM "%s" AS c INNER JOIN "%s" AS t ON t.content_id = c.content_id LEFT OUTER JOIN "content_genres" AS cg ON cg.content_id = c.content_id LEFT OUTER JOIN "genre" AS ge ON ge.genre_id = cg.genre_id GROUP BY t.content_id' % (self.tablename, self.content_type), con=db.engine)

        # Reduce memory
        self.reduce_memory()

        return self.df

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

    def prepare_sim(self):
        """Prepare serie data for content similarity process

        Returns:
            DataFrame: result dataframe
        """
        serie_df = self.get_with_genres()
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
