from src.utils import db
from .content import ContentType

import pandas as pd
import numpy as np


class Genre:
    @staticmethod
    def reduce_memory(genre_df):
        cols = list(genre_df.columns)
        if "genre_id" in cols:
            genre_df["genre_id"] = genre_df["genre_id"].astype("uint16")

        return genre_df

    @classmethod
    def get_genres(cls, types=[]):
        """Get all gernes

        Returns:
            DataFrame: genre dataframe
        """
        if isinstance(types, ContentType):
            types = [types]
        assert all([isinstance(t, ContentType) for t in types]
                   ), "types must be instance of 'ContentType'"

        filt = ''
        if len(types) > 0:
            _types = list(map(lambda x: "'%s'" % str(x).upper(), types))
            filt = 'WHERE content_type IN (%s)' % (', '.join(_types))

        genre_df = pd.read_sql_query(
            'SELECT genre_id, name, content_type FROM "genre" %s' % filt, con=db.engine)

        genre_df = cls.reduce_memory(genre_df)

        return genre_df
