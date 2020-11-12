from src.engines import Popularity, ContentSimilarities, FromUserProfile
from src.engines.engine import Engine

from threading import Thread
from datetime import datetime
from flask import current_app


class Recommend(Engine):
    def train(self):
        p = Popularity()
        p.start()
        p.join()
        c = ContentSimilarities()
        c.start()
        c.join()

        # TODO Recommend content from similarities (from user top rating content)
        # TODO Recommend content from group profile
        fup = FromUserProfile()
        fup.start()
        fup.join()
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
        fup = FromUserProfile(user_uuid=self.user_uuid)
        fup.start()
        fup.join()
        # TODO Collaboratif filtering
