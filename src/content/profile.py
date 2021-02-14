from src.utils import db
from .genre import Genre
from .content import ContentType

import pandas as pd
import numpy as np


class Profile:
    id = "profile_id"
    event_id = "event_id"
    tablename_meta = "recommendation_launched_meta"
    tablename_recommended = "recommendation_launched_result"

    @staticmethod
    def reduce_memory(profile_df):
        cols = list(profile_df.columns)
        if "profile_id" in cols:
            profile_df["profile_id"] = profile_df["profile_id"].astype(
                "uint32")

        return profile_df

    @classmethod
    def get(cls, profile_uuid):
        """Get all users

        Returns:
            DataFrame: user dataframe
        """

        profile_df = pd.read_sql_query(
            'SELECT profile_id FROM "profile" WHERE uuid = \'%s\'' % profile_uuid, con=db.engine)

        return cls.reduce_memory(profile_df)

    @classmethod
    def get_with_genres(cls, profile_uuid, types=[], liked_weight=2):
        """Get users with liked genre

        Args:
            profile_uuid (str): profile uuid.
            types (list|ContentType, optional): str or list of str of genre content type. Defaults to ["APPLICATION", "BOOK", "GAME", "MOVIE", "SERIE", "TRACK"].
            liked_weight (int, optional): Weight of liked genre. Defaults to 2.

        Returns:
            DataFrame: user and liked genre dataframe
        """
        if isinstance(types, ContentType):
            types = [types]
        assert all([isinstance(t, ContentType) for t in types]
                   ), "types must be instance of 'ContentType'"

        filt = ''
        if len(types) > 0:
            _types = list(map(lambda x: "'%s'" % str(x).upper(), types))
            filt = 'AND g.content_type IN (%s)' % (', '.join(_types))

        profile_df = pd.read_sql_query(
            'SELECT p.profile_id as user_id, g.content_type || g.name AS genres FROM "profile" AS p LEFT OUTER JOIN "liked_genres_profile" AS lg ON p.profile_id = lg.profile_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = lg.genre_id %s WHERE uuid = \'%s\'' % (filt, profile_uuid), con=db.engine)

        if profile_df.shape[0] == 0:
            return None

        # Concat liked genre to list
        def list_of_genre(genre_type):
            res = list(genre_type["genres"])
            if len(''.join(res)) == 0:
                return ""
            return res

        profile_df = profile_df.fillna('')
        profile_df = profile_df.groupby(
            "user_id").apply(list_of_genre).reset_index()
        profile_df.rename(columns={0: 'genres'}, inplace=True)

        # reduce memory
        profile_df = cls.reduce_memory(profile_df)

        # get genres list
        genre_df = Genre.get_genres(types)
        genre_df['name'] = genre_df['content_type'] + genre_df['name']
        genre_df.drop(['content_type', 'genre_id'], axis=1, inplace=True)

        result = profile_df.copy()
        result.drop(["genres"], axis=1, inplace=True)

        # For every row in the dataframe, iterate through the list of genres and place a (1 by default or 2) into the corresponding column
        for index, row in profile_df.iterrows():
            for g_index, g_row in genre_df.iterrows():
                if g_row['name'] in row['genres']:
                    result.at[index, g_row['name']] = liked_weight
                else:
                    result.at[index, g_row['name']] = 1

        # Reduce memory
        genre_cols = list(set(result.columns) -
                          set(profile_df.columns))
        for c in genre_cols:
            result[c] = result[c].astype("uint8")

        return result

    @classmethod
    def _reduce_metadata_memory(cls, df: pd.DataFrame):
        cols = list(df.columns)

        if 'profile_id' in cols:
            df['profile_id'] = df['profile_id'].astype("uint32")
        if 'content_id' in cols:
            df['content_id'] = df['content_id'].astype("uint32")
        if 'rating' in cols:
            df['rating'] = df['rating'].fillna(0).astype("uint8")
        if 'review_see_count' in cols:
            df['review_see_count'] = df['review_see_count'].fillna(
                0).astype("uint16")
        if 'count' in cols:
            df['count'] = df['count'].fillna(0).astype("uint16")

        # last_rating_date
        # last_review_see_date
        # last_count_increment

        return df

    @classmethod
    def get_meta(cls, content_class, cols, event_id, limit=None):
        if cols is None:
            cols = content_class.__meta_cols__
        assert all([x in content_class.__meta_cols__ for x in cols])

        limit_filt = ''
        if limit is not None:
            assert limit > 0, "Limit must be greater than 0"
            limit_filt = "LIMIT %s" % limit

        df = pd.read_sql_query('SELECT %s FROM "%s" AS m INNER JOIN "%s" AS c ON c.content_id = m.content_id INNER JOIN "%s" AS ct ON ct.content_id = c.content_id WHERE event_id=\'%s\' %s' % (
            'm.'+', m.'.join(cols), cls.tablename_meta, content_class.tablename, content_class.content_type, event_id, limit_filt), con=db.engine)

        return Profile._reduce_metadata_memory(df)
