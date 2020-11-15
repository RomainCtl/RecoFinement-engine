from src.engines import Popularity, ContentSimilarities, FromProfile, FromSimilarContent
from src.engines.engine import Engine

from threading import Thread
from datetime import datetime
from flask import current_app
import uuid


class Recommend(Engine):
    def train(self):
        start_popularity_engine(wait=True)
        start_similarities_engine(wait=True)

        start_from_similar_content_engine(
            wait=True)  # from user top rating content
        start_from_profile_engine(wait=True)
        # TODO Collaboratif filtering

        start_from_similar_content_engine_for_group(wait=True)
        start_from_profile_engine_for_group(wait=True)


class RecommendUser(Engine):
    def __init__(self, *args, user_uuid, **kwargs):
        super().__init__(*args, **kwargs)

        self.user_uuid = str(user_uuid)

    def train(self):
        start_from_profile_engine(
            wait=True, user_uuid=self.user_uuid)  # the fastest first
        start_from_similar_content_engine(
            wait=True, user_uuid=self.user_uuid)  # from user top rating content
        # TODO Collaboratif filtering


def start_popularity_engine(wait=True):
    p = Popularity()
    p.start()
    if wait:
        p.join()


def start_similarities_engine(wait=True):
    c = ContentSimilarities()
    c.start()
    if wait:
        c.join()


def start_from_profile_engine(wait=True, user_uuid=None):
    fup = FromProfile(user_uuid=user_uuid)
    fup.start()
    if wait:
        fup.join()


def start_from_similar_content_engine(wait=True, user_uuid=None):
    fsc = FromSimilarContent(user_uuid=user_uuid)
    fsc.start()
    if wait:
        fsc.join()


def start_from_profile_engine_for_group(wait=True, group_id=None):
    fup = FromProfile(group_id=group_id, is_group=True)
    fup.start()
    if wait:
        fup.join()


def start_from_similar_content_engine_for_group(wait=True, group_id=None):
    fsc = FromSimilarContent(group_id=group_id, is_group=True)
    fsc.start()
    if wait:
        fsc.join()
