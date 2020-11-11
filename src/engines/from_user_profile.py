from src.content import User, Application, Book, Game, Movie, Serie, Track
from src.utils import db
from .engine import Engine

from datetime import datetime
import pandas as pd
import numpy as np


class FromUserProfile(Engine):
    """(Re-)Set top 50 recommended media (per type) for each user

    The main purpose it to recommend items based on the user profile (contruction of liked genre + explicit liked genres)
    """
    __media__ = {
        # "application": (Application, "app_id", int, "APPLICATION"), # 1 seul genre par app ...
        # "book": (Book, "isbn", str, "BOOK"), # Pas de genre pour les livres
        "game": (Game, "game_id", int, "GAME"),
        "movie": (Movie, "movie_id", int, "MOVIE"),
        "serie": (Serie, "serie_id", int, "SERIE"),
        "track": (Track, "track_id", int, "TRACK"),
    }

    def train(self):
        for media in self.__media__:
            if media != "serie":
                continue
            st_time = datetime.utcnow()

            info = self.__media__[media]

            # Get user
            self.user_df = User.get_with_genres(info[3])

            # Get media (only rating for now)
            self.media_df = info[0].get_for_profile()
            self.mediaWithGenres_df = info[0].prepare_from_user_profile(
                self.media_df)

            self.logger.debug("%s data preparation performed in %s" %
                              (media, datetime.utcnow()-st_time))

            # Now let's get the genres of every movie in our original dataframe
            genre_table = self.mediaWithGenres_df.set_index(
                self.mediaWithGenres_df[info[1]])

            # for each user
            for index, user in self.user_df.iterrows():
                user_input = info[0].get_meta(
                    [info[1], "rating"], user["user_id"])

                user_profile = self.learning_user_profile(
                    user, info[1], user_input)

                user_profile = user_profile.fillna(0)

                # With the input's profile and the complete list of medias and their genres in hand, we're going to take the weighted average of every media based on the input profile and recommend the top twenty medias that most satisfy it.

                # Multiply the genres by the weights and then take the weighted average
                recommendationTable_df = (
                    (genre_table*user_profile).sum(axis=1))/(user_profile.sum())

                # Sort our recommendations in descending order
                recommendationTable_df = recommendationTable_df.sort_values(
                    ascending=False)

                print(user_profile)
                print(recommendationTable_df.head(50))

    def learning_user_profile(self, user, media_id, user_input):
        """Learning user profile from rating and interests

        Args:
            user (Series): user interests and id
            media_id (str): media id in db

        Returns:
            Serie: user profile
        """
        # Filtering out the medias from the input
        users_medias = self.mediaWithGenres_df[self.mediaWithGenres_df[media_id].isin(
            user_input[media_id].tolist())]

        # Resetting the index to avoid future issues
        users_medias = users_medias.reset_index(drop=True)

        # Dropping unnecessary issues due to save memory and to avoid issues
        user_genre_table = users_medias.drop([media_id], axis=1)

        # we're going to turn each genre into weights. We can do this by using the input's reviews and multiplying them into the input's genre table and then summing up the resulting table by column.

        # Dot produt to get weights
        user_profile = user_genre_table.transpose().dot(user_input['rating'])
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
