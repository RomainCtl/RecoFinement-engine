from src.utils import db, clean_data, create_soup

import pandas as pd
import numpy as np


class Track:
    __meta_cols__ = ["user_id", "track_id", "rating",
                     "play_count", "review_see_count", "last_played_date"]

    id = "track_id"
    id_type = int
    tablename_recommended = "recommended_track"
    tablename_similars = "similars_track"
    tablename_media = "track"
    uppername = tablename_media.upper()

    @staticmethod
    def reduce_memory(track_df):
        cols = list(track_df.columns)
        if "track_id" in cols:
            # currently we have only ~ 10_000 track
            track_df["track_id"] = track_df["track_id"].astype("uint16")
        if "rating" in cols:
            track_df["rating"] = track_df["rating"].astype("float32")
        if "rating_count" in cols:
            track_df["rating_count"] = track_df["rating_count"].fillna(0)
            track_df["rating_count"] = track_df["rating_count"].astype(
                "uint32")
        if "popularity_score" in cols:
            track_df["popularity_score"] = track_df["popularity_score"].astype(
                "float32")

        return track_df

    @classmethod
    def get_meta(cls, cols=None, user_id=None):
        """Get user metatrack metadata

        Returns:
            DataFrame: pandas DataFrame
        """
        if cols is None:
            cols = cls.__meta_cols__
        assert all([x in cls.__meta_cols__ for x in cols])

        filt = ''
        if user_id is not None:
            filt = "WHERE user_id = '%s'" % user_id

        df = pd.read_sql_query('SELECT %s FROM "meta_user_track" %s' % (
            ', '.join(cols), filt), con=db.engine)

        # Reduce memory usage for ratings
        if 'user_id' in cols:
            df['user_id'] = df['user_id'].astype("uint32")
        if 'track_id' in cols:
            df['track_id'] = df['track_id'].astype("uint16")
        if 'rating' in cols:
            df['rating'] = df['rating'].fillna(0)
            df['rating'] = df['rating'].astype("uint8")
        if 'play_count' in cols:
            df['play_count'] = df['play_count'].astype("uint16")
        if 'review_see_count' in cols:
            df['review_see_count'] = df['review_see_count'].astype("uint16")

        return df

    @classmethod
    def get_ratings(cls):
        """Get all tracks and their metadata

        Returns:
            DataFrame: track dataframe
        """
        track_df = pd.read_sql_query(
            'SELECT track_id, rating, rating_count FROM "track"', con=db.engine)

        # Reduce memory
        track_df = cls.reduce_memory(track_df)

        return track_df

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

    @classmethod
    def get_with_genres(cls):
        """Get track

        NOTE can add 't.rating' and 't.rating_count' column if we introduce popularity filter to content-based engine
            example: this recommender would take the 30 most similar item, calculate the popularity score and then return the top 10

        Returns:
            DataFrame: dataframe of track data
        """
        track_df = pd.read_sql_query(
            'SELECT t.track_id, t.title, t.year, t.artist_name, t.release, string_agg(g.name, \',\') AS genres FROM "track" AS t LEFT OUTER JOIN "track_genres" AS tg ON tg.track_id = t.track_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY t.track_id', con=db.engine)

        # Reduce memory
        track_df = cls.reduce_memory(track_df)

        return track_df

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

    @staticmethod
    def prepare_sim(track_df):
        """Prepare track data for content similarity process

        Args:
            track_df (DataFrame): Track dataframe

        Returns:
            DataFrame: result dataframe
        """
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
