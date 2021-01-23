from src.utils import db, clean_data, create_soup
from .content import Content, ContentType

import pandas as pd
import numpy as np


class Track(Content):
    content_type = ContentType.TRACK

    def request_for_popularity(self):
        return super().request_for_popularity(self.content_type)

    def calc_popularity_score(self, df):
        # NOTE IMDB measure of popularity does not seem to be relevant for this media.

        # Calculate the minimum number of votes required to be in the chart
        m = df["rating_count"].quantile(0.90)

        # Filter out all qualified media into a new DataFrame
        q_df = df.copy().loc[df['rating_count'] >= m]

        q_df['popularity_score'] = q_df.apply(
            lambda x: float(format(x["rating_count"] + x["rating"], ".4f")), axis=1, result_type="reduce")

        return q_df

    @classmethod
    def get_similars(cls, track_id):
        """Get all similars content of a track

        Args:
            track_id (int): track unique id

        Returns:
            Dataframe: similars track dataframe
        """
        track_df = pd.read_sql_query(
            'SELECT st.track_id0 as track_id, st.track_id1 as similar_track_id, st.similarity, t.popularity_score FROM "similars_track" AS st INNER JOIN "track" AS t ON t.track_id = st.track_id1 WHERE st.track_id0 = \'%s\'' % track_id, con=db.engine)

        track_df = cls.reduce_memory(track_df)

        return track_df

    @classmethod
    def get_for_profile(cls):
        """Get all tracks id

        Returns:
            DataFrame: track dataframe
        """
        track_df = pd.read_sql_query(
            'SELECT t.track_id, string_agg(g.content_type || g.name, \',\') AS genres FROM "track" AS t LEFT OUTER JOIN "track_genres" AS tg ON tg.track_id = t.track_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY t.track_id', con=db.engine)

        # Reduce memory
        track_df = cls.reduce_memory(track_df)

        return track_df

    def get_with_genres(self):
        """Get track

        NOTE can add 't.rating' and 't.rating_count' column if we introduce popularity filter to content-based engine
            example: this recommender would take the 30 most similar item, calculate the popularity score and then return the top 10

        Returns:
            DataFrame: dataframe of track data
        """
        self.df = pd.read_sql_query(
            'SELECT t.content_id, t.title, t.year, t.artist_name, t.release, string_agg(ge.name, \',\') AS genres FROM "%s" AS c INNER JOIN "%s" AS t ON t.content_id = c.content_id LEFT OUTER JOIN "content_genres" AS cg ON cg.content_id = c.content_id LEFT OUTER JOIN "genre" AS ge ON ge.genre_id = cg.genre_id GROUP BY t.content_id' % (self.tablename, self.content_type), con=db.engine)

        # Reduce memory
        self.reduce_memory()

        return self.df

    @staticmethod
    def prepare_from_user_profile(track_df):
        """Get track with genre

        Args:
            track_df (DataFrame): Track dataframe

        Returns:
            DataFrame: track with genre weight (0 or 1)
        """

        # Copying the track dataframe into a new one since we won't need to use the genre information in our first case.
        trackWithGenres_df = track_df.copy()

        # For every row in the dataframe, iterate through the list of genres and place a 1 into the corresponding column
        for index, row in track_df.iterrows():
            if row['genres'] is not None:
                for genre in row['genres'].split(","):
                    trackWithGenres_df.at[index, genre] = 1

        # Filling in the NaN values with 0 to show that a track doesn't have that column's genre
        trackWithGenres_df = trackWithGenres_df.fillna(0)

        # Reduce memory
        genre_cols = list(set(trackWithGenres_df.columns) -
                          set(track_df.columns))
        for c in genre_cols:
            trackWithGenres_df[c] = trackWithGenres_df[c].astype("uint8")

        trackWithGenres_df.drop(["genres"], axis=1, inplace=True)

        return trackWithGenres_df

    def prepare_sim(self):
        """Prepare track data for content similarity process

        Returns:
            DataFrame: result dataframe
        """
        track_df = self.get_with_genres()
        # Transform genres str to list
        track_df["genres"] = track_df["genres"].apply(
            lambda x: str(x).split(","))

        # Remove '0' from year
        track_df["year"] = track_df["year"].astype(str)
        track_df["year"] = track_df["year"].replace('0', '')

        # Replace NaN with an empty string
        features = ['title', 'artist_name', 'release', 'genres']
        for feature in features:
            track_df[feature] = track_df[feature].fillna('')

        # Clean and homogenise data
        features = ['title', 'year', 'artist_name', 'release', 'genres']
        for feature in features:
            track_df[feature] = track_df[feature].apply(clean_data)

        # Transform all list to simple str with space sep
        track_df["genres"] = track_df["genres"].apply(' '.join)
        track_df["genres"] = track_df["genres"].replace('none', '')

        # Create a new soup feature
        track_df['soup'] = track_df.apply(
            lambda x: create_soup(x, features), axis=1)

        # Delete unused cols (feature)
        track_df = track_df.drop(features, 1)

        return track_df
