from src.engines import Popularity, ContentSimilarities, CollaborativeFiltering, FromProfile, FromSimilarContent, LinkBetweenItems
from src.engines.engine import Engine

from threading import Thread
from datetime import datetime
from flask import current_app
import uuid


class Recommend(Engine):
    def train(self):
        start_popularity_engine(wait=True)
        start_similarities_engine(wait=True)
        start_similarities_between_items_engine(wait=True)

        start_from_similar_content_engine(wait=True)
        start_from_profile_engine(wait=True)
        start_collaborative_engine(wait=True)

        start_from_similar_content_engine_for_group(wait=True)
        start_from_profile_engine_for_group(wait=True)


class RecommendUser(Engine):
    def __init__(self, *args, user_uuid, **kwargs):
        super().__init__(*args, **kwargs)

        self.user_uuid = str(user_uuid)

    def train(self):
        start_from_similar_content_engine(wait=True, user_uuid=self.user_uuid)
        start_from_profile_engine(wait=True, user_uuid=self.user_uuid)


class RecommendProfile(Engine):
    def __init__(self, *args, profile_uuid, event_id, **kwargs):
        super().__init__(*args, **kwargs)

        self.profile_uuid = str(profile_uuid)
        self.event_id = event_id

    def train(self):
        start_from_similar_content_engine(
            wait=True, profile_uuid=self.profile_uuid, event_id=self.event_id)
        start_from_profile_engine(
            wait=True, profile_uuid=self.profile_uuid, event_id=self.event_id)


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


def start_similarities_between_items_engine(wait=True):
    c = LinkBetweenItems()
    c.start()
    if wait:
        c.join()


def start_collaborative_engine(wait=True):
    c = CollaborativeFiltering()
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


def start_from_profile_engine_for_profile(profile_uuid, event_id, wait=True):
    fup = FromProfile(profile_uuid=profile_uuid, event_id=event_id)
    fup.start()
    if wait:
        fup.join()


def start_from_similar_content_engine_for_profile(profile_uuid, event_id, wait=True):
    fsc = FromSimilarContent(profile_uuid=profile_uuid, event_id=event_id)
    fsc.start()
    if wait:
        fsc.join()
