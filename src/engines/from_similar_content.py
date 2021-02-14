from src.content import User, Group, Profile
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
    user_uuid = None
    group_id = None

    def __init__(self, *args, user_uuid=None, group_id=None, is_group=False, profile_uuid=None, event_id=None, **kwargs):
        super().__init__(*args, **kwargs)

        self.profile_uuid = profile_uuid
        uuid.UUID(profile_uuid)  # raise exception if bad not a good uuid (v4)
        self.event_id = event_id
        assert (profile_uuid is None and event_id is None) or (
            profile_uuid is not None and event_id is not None), "profile_uuid and event_id must be both None or both not None!"

        self.is_group = False

        if profile_uuid is not None:
            self.obj = Profile
        else:
            self.is_group = is_group
            if self.is_group:
                self.obj = Group
                self.group_id = group_id
            else:
                self.obj = User
                self.user_uuid = user_uuid
                try:
                    uuid.UUID(user_uuid)
                except (ValueError, TypeError):
                    self.user_uuid = None

    def train(self):
        for media in self.__media__:
            st_time = datetime.utcnow()

            m = media(logger=self.logger)

            if self.is_group:
                self.obj_df = self.obj.get(group_id=self.group_id)
            elif self.user_uuid is not None:
                # Get user
                self.obj_df = self.obj.get(user_uuid=self.user_uuid)
            else:
                # Profile
                self.obj_df = self.obj.get(profile_uuid=self.profile_uuid)

            # Check we have a result for this user uuid
            if self.obj_df.shape[0] == 0:
                continue

            len_values = 0
            # for each user (or group, or profile)
            for index, user in self.obj_df.iterrows():
                # Get meta
                meta_cols = [m.id, "rating", "review_see_count"]
                if self.is_group:
                    self.media_df = pd.DataFrame(columns=meta_cols)
                    for u in user['user_id'].split(","):
                        self.media_df = self.media_df.append(
                            m.get_meta(meta_cols, u),
                            ignore_index=True
                        )
                elif self.user_uuid is not None:
                    self.media_df = m.get_meta(meta_cols, user["user_id"])
                else:
                    self.media_df = Profile.get_meta(
                        m, meta_cols, self.event_id)

                # Do not taking bad content that user do not like
                self.media_df = self.media_df[(self.media_df["rating"] >= 3) | (
                    self.media_df["rating"] == 0)]

                # Do not recommend already recommended content
                already_recommended_media = []
                if self.profile_uuid is None:
                    with db as session:
                        result = session.execute('SELECT %s FROM "%s" WHERE %s = \'%s\' AND engine <> \'%s\'' % (
                            m.id, m.tablename_recommended + self.obj.recommended_ext, self.obj.id, user[self.obj.id], self.__class__.__name__))
                        already_recommended_media = [
                            dict(row)[m.id] for row in result]

                # Get list of similars content from already rate content
                similars_df = pd.DataFrame(
                    columns=[m.id, "similar_"+m.id, "similarity", "popularity_score", "rating", "review_see_count"])
                for index, content in self.media_df.iterrows():
                    d = m.get_similars(content[m.id])
                    d = d[~d[m.id].isin(already_recommended_media)]
                    if d.shape[0] > 0:
                        d["rating"] = content["rating"]
                        d["review_see_count"] = content["review_see_count"]
                        similars_df = similars_df.append(
                            d,
                            ignore_index=True,
                        )

                if similars_df.shape[0] > 0:
                    similars_df = similars_df.groupby([m.id, "similar_"+m.id]).agg(
                        {"similarity": "max", "popularity_score": "max", "rating": "sum", "review_see_count": "sum"}).reset_index()

                # Order this list by most popular and make a selection (max popularity_score is 5 (also = max rate), see popularity engine (IMDB formula))
                similars_df["popularity_score"] = similars_df["popularity_score"].fillna(
                    0)

                similars_df["score"] = similars_df["popularity_score"] + \
                    similars_df["similarity"] * similars_df["rating"] + \
                    similars_df["similarity"] * similars_df["review_see_count"]

                # To be between 0 and 1
                similars_df["score"] = similars_df["score"] / \
                    (5 + similars_df["popularity_score"].max())

                similars_df.drop(
                    columns=[m.id, "similarity", "popularity_score", "rating"], axis=1, inplace=True)

                similars_df = similars_df.groupby(
                    ["similar_"+m.id]).sum().reset_index()

                similars_df["score"] = similars_df["score"].apply(
                    lambda x: 1 if x > 1 else x)

                # Store result
                values = []
                for index, item in similars_df.iterrows():
                    if self.profile_uuid is None:
                        values.append(
                            {
                                self.obj.id: int(user[self.obj.id]),
                                m.id: int(item["similar_"+m.id]),
                                "score": float(item["score"]),
                                "engine": self.__class__.__name__,
                                "engine_priority": self.__engine_priority__,
                                "content_type": str(m.content_type).upper(),
                            }
                        )
                    else:
                        values.append(
                            {
                                self.obj.event_id: self.event_id,
                                m.id: int(item["similar_"+m.id]),
                                "score": float(item["score"]),
                                "engine": self.__class__.__name__,
                            }
                        )

                len_values += len(values)

                with db as session:
                    if self.profile_uuid is None:
                        # Reset list of recommended `media`
                        session.execute(
                            text('DELETE FROM "%s" WHERE %s = \'%s\' AND engine = \'%s\' AND content_type = \'%s\'' % (m.tablename_recommended + self.obj.recommended_ext, self.obj.id, user[self.obj.id], self.__class__.__name__, str(m.content_type).upper())))

                        if len(values) > 0:
                            markers = ':%s, :%s, :score, :engine, :engine_priority' % (
                                self.obj.id, m.id)
                            ins = 'INSERT INTO {tablename} VALUES ({markers})'
                            ins = ins.format(
                                tablename=m.tablename_recommended + self.obj.recommended_ext, markers=markers)
                            session.execute(ins, values)
                    else:
                        if len(values) > 0:
                            markers = ':%s, :%s, :score, :engine' % (
                                self.obj.event_id, m.id)
                            ins = 'INSERT INTO {tablename} VALUES ({markers})'
                            ins = ins.format(
                                tablename=self.obj.tablename_recommended, markers=markers)
                            session.execute(ins, values)

            self.logger.info("%s recommendation from similar content in %s (%s lines)" % (
                m.content_type, datetime.utcnow()-st_time, len_values))
