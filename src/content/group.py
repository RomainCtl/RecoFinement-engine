from src.utils import db

import pandas as pd
import numpy as np


class Group:
    @staticmethod
    def reduce_memory(group_df):
        cols = list(group_df.columns)
        if "group_id" in cols:
            group_df["group_id"] = group_df["group_id"].astype("uint32")
        if "genre_id" in cols:
            group_df["genre_id"] = group_df["genre_id"].astype("uint16")

        return group_df

    @classmethod
    def get_genres(cls, types=[]):
        """Get all gernes

        Returns:
            DataFrame: genre dataframe
        """
        accepted_types = ["APPLICATION", "BOOK",
                          "GAME", "MOVIE", "SERIE", "TRACK"]

        if type(types) == str:
            types = [types]
        assert all([t in accepted_types for t in types])

        filt = ''
        if len(types) > 0:
            _types = list(map(lambda x: "'%s'" % x, types))
            filt = 'WHERE content_type IN (%s)' % (', '.join(_types))

        genre_df = pd.read_sql_query(
            'SELECT genre_id, name, content_type FROM "genre" %s' % filt, con=db.engine)

        genre_df = cls.reduce_memory(genre_df)

        return genre_df

    @classmethod
    def get_with_genres(cls, types=[], liked_weight=2, group_id=None):
        accepted_types = ["APPLICATION", "BOOK",
                          "GAME", "MOVIE", "SERIE", "TRACK"]

        if type(types) == str:
            types = [types]
        assert all([t in accepted_types for t in types])

        filt = ''
        if len(types) > 0:
            _types = list(map(lambda x: "'%s'" % x, types))
            filt = 'WHERE g.content_type IN (%s)' % (', '.join(_types))

        df = pd.read_sql_query(
            'SELECT u.group_id, g.content_type || g.name AS genres, count(g.content_type || g.name) ' +
            'FROM (' +
            'SELECT g.group_id, u.user_id FROM "group" AS g INNER JOIN "user" AS u ON u.user_id = g.owner_id' +
            ' UNION ' +
            'SELECT gm.group_id, gm.user_id FROM "group_members" AS gm) AS u ' +
            'LEFT OUTER JOIN "liked_genres" AS lg ON u.user_id = lg.user_id ' +
            'LEFT OUTER JOIN "genre" AS g ON g.genre_id = lg.genre_id ' +
            '%s GROUP BY u.group_id, genres' % filt, con=db.engine)

        if df.shape[0] == 0:
            return None

        # Concat liked genre to list
        def list_of_genre(genre_type):
            res = list(genre_type["genres"])
            if len(''.join(res)) == 0:
                return ""
            return res

        group_df = df.copy().fillna('')
        group_df = group_df.groupby("group_id").apply(
            list_of_genre).reset_index()
        group_df.rename(columns={0: 'genres'}, inplace=True)

        # reduce memory
        group_df = cls.reduce_memory(group_df)

        # get genres list
        genre_df = cls.get_genres(types)
        genre_df['name'] = genre_df['content_type'] + genre_df['name']
        genre_df.drop(['content_type', 'genre_id'], axis=1, inplace=True)

        result = group_df.copy()
        result.drop(["genres"], axis=1, inplace=True)

        # For every row in the dataframe, iterate through the list of genres and place a (1 by default or 2) into the corresponding column
        for index, row in group_df.iterrows():
            for g_index, g_row in genre_df.iterrows():
                if g_row['name'] in row['genres']:
                    result.at[index, g_row['name']] = liked_weight * int(
                        df[
                            (df['group_id'] == row["group_id"]) &
                            (df['genres'] == g_row['name'])
                        ]['count']
                    )
                else:
                    result.at[index, g_row['name']] = 1

        # Reduce memory
        genre_cols = list(set(result.columns) -
                          set(group_df.columns))
        for c in genre_cols:
            result[c] = result[c].astype("uint8")

        return result
