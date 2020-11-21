from src.content import User, Group
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

    def __init__(self, *args, user_uuid=None, group_id=None, is_group=False, **kwargs):
        super().__init__(*args, **kwargs)

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

            if self.is_group:
                self.obj_df = self.obj.get(group_id=self.group_id)
            else:
                # Get user
                self.obj_df = self.obj.get(user_uuid=self.user_uuid)

            # Check we have a result for this user uuid
            if self.obj_df.shape[0] == 0:
                continue

            len_values = 0
            # for each user
            for index, user in self.obj_df.iterrows():
                # Get meta
                meta_cols = [media.id, "rating", "review_see_count"]
                if self.is_group:
                    self.media_df = pd.DataFrame(columns=meta_cols)
                    for u in user['user_id'].split(","):
                        self.media_df = self.media_df.append(
                            media.get_meta(meta_cols, u),
                            ignore_index=True
                        )
                else:
                    self.media_df = media.get_meta(meta_cols, user["user_id"])

                # Do not taking bad content that user do not like
                self.media_df = self.media_df[(self.media_df["rating"] >= 3) | (
                    self.media_df["rating"] == 0)]

                # Do not recommend already recommended content
                already_recommended_media = []
                with db as session:
                    result = session.execute('SELECT %s FROM "%s" WHERE %s = \'%s\' AND engine <> \'%s\'' % (
                        media.id, media.tablename_recommended + self.obj.recommended_ext, self.obj.id, user[self.obj.id], self.__class__.__name__))
                    already_recommended_media = [
                        dict(row)[media.id] for row in result]

                # Get list of similars content from already rate content
                similars_df = pd.DataFrame(
                    columns=[media.id, "similar_"+media.id, "similarity", "popularity_score", "rating", "review_see_count"])
                for index, m in self.media_df.iterrows():
                    d = media.get_similars(m[media.id])
                    d = d[~d[media.id].isin(already_recommended_media)]
                    if d.shape[0] > 0:
                        d["rating"] = m["rating"]
                        d["review_see_count"] = m["review_see_count"]
                        similars_df = similars_df.append(
                            d,
                            ignore_index=True,
                        )

                similars_df = similars_df.groupby(["app_id",  "similar_app_id"]).agg(
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

                similars_df["score"] = similars_df["score"].apply(
                    lambda x: 1 if x > 1 else x)

                similars_df.drop(
                    columns=[media.id, "similarity", "popularity_score", "rating"], axis=1, inplace=True)

                # Store result
                values = []
                for index, item in similars_df.iterrows():
                    values.append(
                        {
                            self.obj.id: int(user[self.obj.id]),
                            media.id: media.id_type(item["similar_"+media.id]),
                            "score": float(item["score"]),
                            "engine": self.__class__.__name__,
                            "engine_priority": self.__engine_priority__,
                        }
                    )

                len_values += len(values)

                with db as session:
                    # Reset list of recommended `media`
                    session.execute(
                        text('DELETE FROM "%s" WHERE %s = \'%s\' AND engine = \'%s\'' % (media.tablename_recommended + self.obj.recommended_ext, self.obj.id, user[self.obj.id], self.__class__.__name__)))

                    if len(values) > 0:
                        markers = ':%s, :%s, :score, :engine, :engine_priority' % (
                            self.obj.id, media.id)
                        ins = 'INSERT INTO {tablename} VALUES ({markers})'
                        ins = ins.format(
                            tablename=media.tablename_recommended + self.obj.recommended_ext, markers=markers)
                        session.execute(ins, values)

            self.logger.info("%s recommendation from similar content in %s (%s lines)" % (
                media.uppername, datetime.utcnow()-st_time, len_values))
