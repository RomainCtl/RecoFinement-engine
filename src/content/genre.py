from src.utils import db

import pandas as pd
import numpy as np


class Genre:
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