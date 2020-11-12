from src.content import User
from src.utils import db
from .engine import Engine

from datetime import datetime
from sqlalchemy import text
import pandas as pd
import numpy as np
import uuid


class FromSimilarContent(Engine):
    """(Re-)Set top similars content per user

    The main purpose it to recommend similar items based on the user liked content
    """
    __engine_priority__ = 2

    def __init__(self, *args, user_uuid=None, **kwargs):
        super().__init__(*args, **kwargs)

        self.user_uuid = user_uuid
        try:
            uuid.UUID(user_uuid)
        except (ValueError, TypeError):
            self.user_uuid = None

    def train(self):
        for media in self.__media__:
            st_time = datetime.utcnow()

            # Get user
            self.user_df = User.get_users(user_uuid=self.user_uuid)

            # Check we have a result for this user uuid
            if self.user_df.shape[0] == 0:
                continue

            len_values = 0
            # for each user
            for index, user in self.user_df.iterrows():
                # Get media (only rating for now)
                self.media_df = media.get_for_profile(
                    [media.id, "rating"], user["user_id"])

                # Get list of similars content from already rate content
                # Order this list by most popular and make a selection

                # Store result
                recommendationTable_df = None

                values = []
                for id, score in recommendationTable_df.items():
                    values.append(
                        {
                            "user_id": int(user["user_id"]),
                            media.id: media.id_type(id),
                            "score": float(score),
                            "engine": self.__class__.__name__,
                            "engine_priority": self.__engine_priority__,
                        }
                    )

                len_values += len(values)

                with db as session:
                    # Reset list of recommended `media`
                    session.execute(
                        text('DELETE FROM "%s" WHERE user_id = \'%s\' AND engine = \'%s\'' % (media.tablename_recommended, user["user_id"], self.__class__.__name__)))

                    markers = ':user_id, :%s, :score, :engine, :engine_priority' % (
                        media.id)
                    ins = 'INSERT INTO {tablename} VALUES ({markers})'
                    ins = ins.format(
                        tablename=media.tablename_recommended, markers=markers)
                    session.execute(ins, values)

            self.logger.info("%s recommendation from similar content in %s (%s lines)" % (
                media.uppername, datetime.utcnow()-st_time, len_values))
