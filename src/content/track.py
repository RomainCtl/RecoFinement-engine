from src.utils import DB
import pandas as pd

_TRACK_COLS = ["track_id", "title", "year",
               "artist_name", "release", "rating", "rating_count"]
_META_COLS = ["user_id", "track_id", "rating",
              "play_count", "review_see_count", "last_played_date"]


class Track(object):
    # TRACK: track_id, title, year, artist_name, release, track_mmid, recording_mbid, rating, rating_count, spotify_id, covert_art_url
    # META: user_id, track_id, rating, play_count, review_see_count, last_played_date
    # TRACK_GENRE: track_id, genre_id
    # GENRE: genre_id, name, count, content_type
    __columns__ = _TRACK_COLS
    __meta_cols__ = _META_COLS

    @classmethod
    def get_meta(cls, cols=_META_COLS):
        """Get user metatrack metadata

        Returns:
            DataFrame: pandas DataFrame
        """
        assert all([x in cls.__meta_cols__ for x in cols])
        # , index_col=["track_id", "user_id"])
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
    def get(cls):
        """Get track ordered by popularity

        Returns:
            DataFrame: dataframe of track data ordered by popularity
        """
        track_df = pd.read_sql_query(
            'SELECT track.track_id, title, string_agg(g.name, \',\') AS genres FROM "track" LEFT OUTER JOIN "track_genres" AS tg ON tg.track_id = track.track_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = tg.genre_id GROUP BY track.track_id ORDER BY rating_count DESC, rating DESC', con=DB.engine)

        # Reduce memory
        track_df["track_id"] = track_df["track_id"].astype(
            "uint16")  # currently we have only ~ 10_000 track

        return track_df

    @classmethod
    def get_with_genres(cls):
        """Get track with genre ordered by popularity

        (At the end, if we do not have any recommendation for a user, the algo will return the most popular track)

        NOTE this pre-processing ('with genre') take somes times, maybe we should directly store all genre as track table column (like output). Or maybe note to keep a dynamic genre list.

        Returns:
            DataFrame, DataFrame: first with is the base one, and second with genre
        """
        track_df = cls.get()

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

        return track_df, trackWithGenres_df
