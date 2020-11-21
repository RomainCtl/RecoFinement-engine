from src.utils import db
from .genre import Genre

import pandas as pd
import numpy as np


class Group:
    id = "group_id"
    recommended_ext = "_for_group"

    @staticmethod
    def reduce_memory(group_df):
        cols = list(group_df.columns)
        if "group_id" in cols:
            group_df["group_id"] = group_df["group_id"].astype("uint32")
        if "genre_id" in cols:
            group_df["genre_id"] = group_df["genre_id"].astype("uint16")

        return group_df

    @classmethod
    def get(cls, group_id=None):
        """Get all groups and members list

        Args:
            group_id (int, optional): group unique id. Defaults to None.

        Returns:
            Dataframe: group dataframe ["group_id", "users_ids"]
        """
        grp = ''
        if group_id is not None:
            grp = "WHERE g.group_id = '%s'" % group_id

        group_df = pd.read_sql_query(
            'SELECT g.group_id, string_agg(g.user_id::varchar, \',\') AS user_id FROM ' +
            '(SELECT g.group_id, u.user_id FROM "group" AS g INNER JOIN "user" AS u ON u.user_id = g.owner_id ' +
            'UNION SELECT gm.group_id, gm.user_id FROM "group_members" AS gm) AS g %s GROUP BY g.group_id' % grp, con=db.engine)

        group_df = cls.reduce_memory(group_df)

        return group_df

    @classmethod
    def get_with_genres(cls, types=[], liked_weight=2, group_id=None):
        """Get groups with liked genre

        Args:
            types (list|str, optional): str or list of str of genre content type. Defaults to ["APPLICATION", "BOOK", "GAME", "MOVIE", "SERIE", "TRACK"]. Defaults to [].
            liked_weight (int, optional): Weight of liked genre. Defaults to 2.
            group_id (int, optional): group unique id. Defaults to None.

        Returns:
            DataFrame: group and liked genre dataframe
        """
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
            'SELECT u.group_id, u.user_id, g.content_type || g.name AS genres, count(g.content_type || g.name) ' +
            'FROM (' +
            'SELECT g.group_id, u.user_id FROM "group" AS g INNER JOIN "user" AS u ON u.user_id = g.owner_id' +
            ' UNION ' +
            'SELECT gm.group_id, gm.user_id FROM "group_members" AS gm) AS u ' +
            'LEFT OUTER JOIN "liked_genres" AS lg ON u.user_id = lg.user_id ' +
            'LEFT OUTER JOIN "genre" AS g ON g.genre_id = lg.genre_id ' +
            '%s GROUP BY u.group_id, u.user_id, genres' % filt, con=db.engine)

        if df.shape[0] == 0:
            return None

        def list_of(c):
            return list(set(c))

        group_df = df.copy().fillna('')
        group_df = group_df.groupby("group_id").agg(
            {'user_id': list_of, 'genres': list_of}).reset_index()
        group_df.rename(columns={0: 'genres'}, inplace=True)

        # reduce memory
        group_df = cls.reduce_memory(group_df)

        # get genres list
        genre_df = Genre.get_genres(types)
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
                        ]['count'].sum()
                    )
                else:
                    result.at[index, g_row['name']] = 1

        # Reduce memory
        genre_cols = list(set(result.columns) -
                          set(group_df.columns))
        for c in genre_cols:
            result[c] = result[c].astype("uint8")

        return result
