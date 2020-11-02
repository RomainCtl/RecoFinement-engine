from src.utils import DB
import pandas as pd


class Track:
    # TRACK: track_id, title, year, artist_name, release, track_mmid, recording_mbid, rating, rating_count, spotify_id, covert_art_url, popularity_score
    # META: user_id, track_id, rating, play_count, review_see_count, last_played_date
    # TRACK_GENRE: track_id, genre_id
    # GENRE: genre_id, name, count, content_type
    __meta_cols__ = ["user_id", "track_id", "rating",
                     "play_count", "review_see_count", "last_played_date"]

    @staticmethod
    def reduce_memory(track_df):
        cols = list(track_df.columns)
        if "track_id" in cols:
            # currently we have only ~ 10_000 track
            track_df["track_id"] = track_df["track_id"].astype("uint16")
        if "year" in cols:
            track_df["year"] = track_df["year"].astype("uint16")
        if "rating" in cols:
            track_df["rating"] = track_df["rating"].astype("float32")
        if "rating_count" in cols:
            track_df["rating_count"] = track_df["rating_count"].astype(
                "uint32")
        if "popularity_score" in cols:
            track_df["popularity_score"] = track_df["popularity_score"].astype(
                "float32")

        return track_df

    @classmethod
    def get_meta(cls, cols=None):
        """Get user metatrack metadata

        Returns:
            DataFrame: pandas DataFrame
        """
        if cols is None:
            cols = cls.__meta_cols__
        assert all([x in cls.__meta_cols__ for x in cols])

        df = pd.read_sql_query('SELECT %s FROM "meta_user_track"' % (
            ', '.join(cols)), con=DB.engine)

        # Reduce memory usage for ratings
        if 'user_id' in cols:
            df['user_id'] = df['user_id'].astype("uint32")
        if 'track_id' in cols:
            df['track_id'] = df['track_id'].astype("uint16")
        if 'rating' in cols:
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
            'SELECT track_id, rating, rating_count FROM "track"', con=DB.engine)

        # Reduce memory
        track_df = cls.reduce_memory(track_df)

        return track_df

    @classmethod
    def get_tracks(cls):
        """Get all tracks and their metadata

        Returns:
            DataFrame: track dataframe
        """
        track_df = pd.read_sql_query('SELECT * FROM "track"', con=DB.engine)

        # Reduce memory
        track_df = cls.reduce_memory(track_df)

        return track_df

    @classmethod
    def get(cls):
        """Get track #ordered by popularity

        #(At the end, if we do not have any recommendation for a user, the algo will return the most popular track)

        Returns:
            DataFrame: dataframe of track data ordered by popularity
        """
        #  ORDER BY rating_count DESC, rating DESC
        track_df = pd.read_sql_query(
            'SELECT track.track_id, title, string_agg(g.name, \',\') AS genres FROM "track" LEFT OUTER JOIN "track_genres" AS tg ON tg.track_id = track.track_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY track.track_id', con=DB.engine)

        # Reduce memory
        track_df = cls.reduce_memory(track_df)

        return track_df

    @staticmethod
    def get_with_genres(track_df):
        """Get track with genre

        NOTE this pre-processing ('with genre') take somes times, maybe we should directly store all genre as track table column (like output). Or maybe note to keep a dynamic genre list.

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

        return trackWithGenres_df
