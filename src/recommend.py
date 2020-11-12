from src.engines import Popularity, ContentSimilarities, FromUserProfile
from src.engines.engine import Engine

from threading import Thread
from datetime import datetime
from flask import current_app


class Recommend(Engine):
    def train(self):
        start_popularity_engine(wait=True)
        start_similarities_engine(wait=True)

        # TODO Recommend content from similarities (from user top rating content)
        # TODO Recommend content from group profile
        start_from_user_profile_engine(wait=True)
        # TODO Collaboratif filtering


class RecommendUser(Engine):
    def __init__(self, *args, user_uuid, **kwargs):
        super().__init__(*args, **kwargs)

        self.user_uuid = user_uuid

        # raise an ValueError if invalid uuid format
        uuid.UUID(user_uuid)

    def train(self):
        # TODO Recommend content from similarities (from user top rating content)
        # TODO Recommend content from group profile
        start_from_user_profile_engine(wait=True)
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


def start_from_user_profile_engine(wait=True):
    fup = FromUserProfile()
    fup.start()
    if wait:
        fup.join()
