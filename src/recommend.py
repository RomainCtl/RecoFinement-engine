from src.engines import Popularity, ContentSimilarities, FromUserProfile

from threading import Thread
from datetime import datetime
from flask import current_app


class Recommend(Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app = current_app._get_current_object()
        self.logger = current_app.logger

    def run(self):
        st_time = datetime.utcnow()

        Popularity().start().join()
        ContentSimilarities().start().join()

        # TODO Recommend content from similarities (from user top rating content)
        # TODO Recommend content from group profile
        FromUserProfile().start().join()
        # TODO Collaboratif filtering

        self.logger.info("%s engine performed in %s" %
                         (self.__class__.__name__, datetime.utcnow()-st_time))


class RecommendUser(Thread):
    def __init__(self, *args, user_uuid, **kwargs):
        super().__init__(*args, **kwargs)
        self.app = current_app._get_current_object()
        self.logger = current_app.logger

        self.user_uuid = user_uuid

        # raise an ValueError if invalid uuid format
        uuid.UUID(user_uuid)

    def run(self):
        st_time = datetime.utcnow()

        # TODO Recommend content from similarities (from user top rating content)
        # TODO Recommend content from group profile
        FromUserProfile(user_uuid=self.user_uuid).start().join()
        # TODO Collaboratif filtering

        self.logger.info("%s engine performed in %s" %
                         (self.__class__.__name__, datetime.utcnow()-st_time))
