from src.content import User, Group, Profile, ContentType
from src.utils import db
from .engine import Engine

from datetime import datetime
from sqlalchemy import text
import pandas as pd
import numpy as np
import uuid


class FromProfile(Engine):
    """(Re-)Set top recommended media (per type) for each user (or group)

    The main purpose it to recommend items based on the profile of a user or a group (contruction of liked genre + explicit liked genres)
    """
    __engine_priority__ = 4
    user_uuid = None
    group_id = None

    def __init__(self, *args, user_uuid=None, group_id=None, is_group=False, profile_uuid=None, event_id=None, **kwargs):
        """
        Args:
            user_uuid (uuid|str, optional): user uuid to start engine for this specific user. Defaults to None.
            is_group (bool, optional): if engine will work for user profile or group profile. Defaults to False.
        """
        super().__init__(*args, **kwargs)

        assert (profile_uuid is None and event_id is None) or (
            profile_uuid is not None and event_id is not None), "profile_uuid and event_id must be both None or both not None!"

        self.is_group = False
        self.user_uuid = user_uuid

        self.profile_uuid = profile_uuid
        if profile_uuid is not None:
            self.obj = Profile
            # raise exception if bad not a good uuid (v4)
            uuid.UUID(profile_uuid)
            self.event_id = event_id
            self.user_uuid = None
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
        necessary_for, necessary_for_media_id, necessary_for_user_id = self.check_if_necessary()
        for media in necessary_for:
            m = media(logger=self.logger)
            if m.content_type in [
                ContentType.APPLICATION,  # 1 seul genre par app ...
                ContentType.BOOK  # Pas de genre pour les livres
            ]:
                continue
            st_time = datetime.utcnow()

            if self.is_group:
                self.obj_df = self.obj.get_with_genres(
                    types=m.content_type, group_id=self.group_id)
            elif self.user_uuid is not None:
                # Get user
                self.obj_df = self.obj.get_with_genres(
                    types=m.content_type, user_uuid=self.user_uuid, user_id_list=necessary_for_user_id[str(m.content_type)])
            else:
                # Profile
                self.obj_df = self.obj.get_with_genres(
                    types=m.content_type, profile_uuid=self.profile_uuid)

            # Check we have a result for this user uuid
            if self.obj_df is None:
                continue

            # Get media (only rating for now)
            self.media_df = m.get_for_profile()
            self.mediaWithGenres_df = m.prepare_from_user_profile(
                self.media_df)

            self.logger.debug("%s data preparation performed in %s" %
                              (m.content_type, datetime.utcnow()-st_time))

            # Now let's get the genres of every movie in our original dataframe
            genre_table = self.mediaWithGenres_df.set_index(
                self.mediaWithGenres_df[m.id])

            len_values = 0
            # for each user
            for index, user in self.obj_df.iterrows():
                # Get meta
                meta_cols = [m.id, "rating", "review_see_count"]
                if self.is_group:
                    user_input = pd.DataFrame(columns=meta_cols)
                    for u in user['user_id']:
                        user_input = user_input.append(
                            m.get_meta(
                                meta_cols, u, list_of_content_id=necessary_for_media_id[str(m.content_type)]),
                            ignore_index=True
                        )
                elif self.user_uuid is not None:
                    user_input = m.get_meta(
                        meta_cols, user["user_id"], list_of_content_id=necessary_for_media_id[str(m.content_type)])
                else:
                    user_input = Profile.get_meta(
                        m, meta_cols, self.event_id)

                if user_input.shape[0] == 0:
                    continue

                user_profile = self.learning_user_profile(
                    user, m.id, user_input)

                user_profile = user_profile.fillna(0)

                # Case if user do not have any preferences for this media (0 rating and 0 interests)
                if user_profile.sum() == 0:
                    continue

                # With the input's profile and the complete list of medias and their genres in hand, we're going to take the weighted average of every media based on the input profile and recommend the top twenty medias that most satisfy it.

                # Multiply the genres by the weights and then take the weighted average
                recommendationTable_df = (
                    (genre_table*user_profile).sum(axis=1))/(user_profile.sum())

                # Do not recommend already recommended content
                already_recommended_media = []
                if self.profile_uuid is None:
                    with db as session:
                        result = session.execute('SELECT %s FROM "%s" WHERE %s = \'%s\' AND engine <> \'%s\'' % (
                            m.id, m.tablename_recommended + self.obj.recommended_ext, self.obj.id, user[self.obj.id], self.__class__.__name__))
                        already_recommended_media = [
                            dict(row)[m.id] for row in result]

                recommendationTable_df = recommendationTable_df[~recommendationTable_df.index.isin(
                    already_recommended_media)]

                # Sort our recommendations in descending order
                recommendationTable_df = recommendationTable_df.sort_values(
                    ascending=False)

                maxi = recommendationTable_df.iloc[0]
                mini = maxi - 0.2

                # Get first 200 (if filter give too much data)
                recommendationTable_df = recommendationTable_df[recommendationTable_df >= mini][:200]

                values = []
                for id, score in recommendationTable_df.items():
                    if self.profile_uuid is None:
                        values.append(
                            {
                                self.obj.id: int(user[self.obj.id]),
                                m.id: int(id),
                                "score": float(score),
                                "engine": self.__class__.__name__,
                                "engine_priority": self.__engine_priority__,
                                "content_type": str(m.content_type).upper(),
                            }
                        )
                    else:
                        values.append(
                            {
                                self.obj.event_id: self.event_id,
                                m.id: int(id),
                                "score": float(score),
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
                            markers = ':%s, :%s, :score, :engine, :engine_priority, :content_type' % (
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

            self.logger.info("%s recommendation from user profile performed in %s (%s lines)" % (
                m.content_type, datetime.utcnow()-st_time, len_values))

    def learning_user_profile(self, user, media_id, user_input):
        """Learning user profile from rating and interests

        Args:
            user (Series): user interests and id
            media_id (str): media id in db

        Returns:
            Serie: user profile
        """
        user_input = user_input.groupby([media_id]).sum().reset_index()

        # Filtering out the medias from the input
        users_medias = self.mediaWithGenres_df[self.mediaWithGenres_df[media_id].isin(
            user_input[media_id].tolist())]

        # Filtering input if not in media
        user_input = user_input[user_input[media_id].isin(
            users_medias[media_id].tolist())]

        # Resetting the index to avoid future issues
        users_medias = users_medias.reset_index(drop=True)
        user_input = user_input.reset_index(drop=True)

        # Dropping unnecessary issues due to save memory and to avoid issues
        user_genre_table = users_medias.drop([media_id], axis=1)

        # we're going to turn each genre into weights. We can do this by using the input's reviews and multiplying them into the input's genre table and then summing up the resulting table by column.

        # Dot produt to get weights
        user_profile = user_genre_table.transpose()\
            .dot(user_input['rating'] + user_input['review_see_count'])
        user_profile = user_profile.astype("float32")

        # Take into account explicit user interests
        user_interests = user.copy()
        user_interests.drop(["user_id"], inplace=True)

        user_profile = user_profile.apply(lambda x: 1.0 if x == 0 else x)
        user_profile = user_profile.mul(
            user_interests, level=list(user_interests.keys()))
        user_profile = user_profile.apply(lambda x: 0.0 if x == 1 else x)
        user_profile = user_profile.astype("float32")

        # Now, we have the weights for every of the user's preferences.
        return user_profile

    def check_if_necessary(self):
        if self.profile_uuid is not None:
            return self.__media__, [], []

        if self.user_uuid is not None:
            user_id = self.obj.get(user_uuid=self.user_uuid)[0]["user_id"]

        necessary_for = []
        necessary_for_media_id = {}
        necessary_for_user_id = {}
        for media in self.__media__:
            df = pd.read_sql_query(
                'SELECT last_launch_date FROM "engine" WHERE engine = \'%s\' AND content_type = \'%s\'' % (self.__class__.__name__, str(media.content_type).upper()), con=db.engine)

            if df.shape[0] == 0:
                # means that this engine has never been launched.
                necessary_for.append(media)
                continue

            last_launch_date = df.iloc[0]["last_launch_date"]

            necessary_for_media_id[str(media.content_type)] = []
            necessary_for_user_id[str(media.content_type)] = []

            if self.user_uuid is not None:
                # launched only for one group or one user
                # check if this user have new interaction (meta_...), if news => launch
                df = pd.read_sql_query(
                    'SELECT occured_by AS user_id FROM "meta_added_event" WHERE occured_at > \'%s\' AND occured_by = \'%s\'' % (last_launch_date, user_id) +
                    'UNION SELECT occured_by AS user_id FROM "changed_event" WHERE model_name = \'meta_user_content\' AND occured_at > \'%s\' AND occured_by = \'%s\'' % (last_launch_date, user_id), con=db.engine)

                if df.shape[0] != 0:
                    necessary_for.append(media)
                    continue

                # if not, check if new media, if news => launch only for these medias (return list of new content_id)
                df = pd.read_sql_query(
                    'SELECT object_id as content_id FROM "%s_added_event" WHERE occured_at > \'%s\'' % (media.content_type, last_launch_date), con=db.engine)

                if df.shape[0] != 0:
                    necessary_for_media_id[str(
                        media.content_type)] = df['content_id'].to_list()
                    necessary_for.append(media)

            elif self.group_id is not None:
                # TODO check news interactions for group too
                necessary_for.append(media)

                # if not, check if new media, if news => launch only for these medias (return list of new content_id)
                df = pd.read_sql_query(
                    'SELECT object_id as content_id FROM "%s_added_event" WHERE occured_at > \'%s\'' % (media.content_type, last_launch_date), con=db.engine)

                if df.shape[0] != 0:
                    necessary_for_media_id[str(
                        media.content_type)] = df['content_id'].to_list()
                    necessary_for.append(media)
            else:
                # is for all
                # if new media, launch for all user
                df = pd.read_sql_query(
                    'SELECT object_id as content_id FROM "%s_added_event" WHERE occured_at > \'%s\'' % (media.content_type, last_launch_date), con=db.engine)

                if df.shape[0] != 0:
                    necessary_for_media_id[str(
                        media.content_type)] = df['content_id'].to_list()
                    necessary_for.append(media)
                    continue

                # else, select user with interaction
                df = pd.read_sql_query(
                    'SELECT occured_by AS user_id FROM "meta_added_event" WHERE occured_at > \'%s\'' % last_launch_date +
                    'UNION SELECT occured_by AS user_id FROM "changed_event" WHERE model_name = \'meta_user_content\' AND occured_at > %s' % last_launch_date, con=db.engine)

                if df.shape[0] != 0:
                    necessary_for_user_id[str(
                        media.content_type)] = df['user_id'].to_list()
                    necessary_for.append(media)

        return necessary_for, necessary_for_media_id, necessary_for_user_id
