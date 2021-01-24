from src.utils import db
from .genre import Genre
from .content import ContentType

import pandas as pd
import numpy as np


class User:
    id = "user_id"
    recommended_ext = ""

    @staticmethod
    def reduce_memory(user_df):
        cols = list(user_df.columns)
        if "user_id" in cols:
            user_df["user_id"] = user_df["user_id"].astype("uint32")
        if "genre_id" in cols:
            user_df["genre_id"] = user_df["genre_id"].astype("uint16")

        return user_df

    @classmethod
    def get(cls, user_uuid=None):
        """Get all users

        NOTE we recover only the real users, not those recovered via datasets.

        Returns:
            DataFrame: user dataframe
        """
        usr = ''
        if user_uuid is not None:
            usr = "AND uuid = '%s'" % user_uuid

        user_df = pd.read_sql_query(
            'SELECT user_id FROM "user" WHERE password_hash <> \'no_pwd\' %s' % usr, con=db.engine)

        user_df = cls.reduce_memory(user_df)

        return user_df

    @classmethod
    def get_with_genres(cls, types=[], liked_weight=2, user_uuid=None):
        """Get users with liked genre

        Args:
            types (list|ContentType, optional): str or list of str of genre content type. Defaults to ["APPLICATION", "BOOK", "GAME", "MOVIE", "SERIE", "TRACK"].
            liked_weight (int, optional): Weight of liked genre. Defaults to 2.
            user_uuid (str, optional): user uuid. Defaults to None.

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

        usr = ''
        if user_uuid is not None:
            usr = "AND u.uuid = '%s'" % user_uuid

        user_df = pd.read_sql_query(
            'SELECT u.user_id, g.content_type || g.name AS genres FROM "user" AS u LEFT OUTER JOIN "liked_genres" AS lg ON u.user_id = lg.user_id LEFT OUTER JOIN "genre" AS g ON g.genre_id = lg.genre_id %s WHERE password_hash <> \'no_pwd\' %s' % (filt, usr), con=db.engine)

        if user_df.shape[0] == 0:
            return None

        # Concat liked genre to list
        def list_of_genre(genre_type):
            res = list(genre_type["genres"])
            if len(''.join(res)) == 0:
                return ""
            return res

        user_df = user_df.fillna('')
        user_df = user_df.groupby("user_id").apply(list_of_genre).reset_index()
        user_df.rename(columns={0: 'genres'}, inplace=True)

        # reduce memory
        user_df = cls.reduce_memory(user_df)

        # get genres list
        genre_df = Genre.get_genres(types)
        genre_df['name'] = genre_df['content_type'] + genre_df['name']
        genre_df.drop(['content_type', 'genre_id'], axis=1, inplace=True)

        result = user_df.copy()
        result.drop(["genres"], axis=1, inplace=True)

        # For every row in the dataframe, iterate through the list of genres and place a (1 by default or 2) into the corresponding column
        for index, row in user_df.iterrows():
            for g_index, g_row in genre_df.iterrows():
                if g_row['name'] in row['genres']:
                    result.at[index, g_row['name']] = liked_weight
                else:
                    result.at[index, g_row['name']] = 1

        # Reduce memory
        genre_cols = list(set(result.columns) -
                          set(user_df.columns))
        for c in genre_cols:
            result[c] = result[c].astype("uint8")

        return result
