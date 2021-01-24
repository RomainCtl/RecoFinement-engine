from src.utils import db, clean_data, create_soup
from .content import Content, ContentType

import pandas as pd
import numpy as np


class Book(Content):
    content_type = ContentType.BOOK

    def request_for_popularity(self):
        return super().request_for_popularity(self.content_type)

    def get_with_genres(self):
        """Get book

        NOTE can add 't.rating' and 't.rating_count' column if we introduce popularity filter to content-based engine
            example: this recommender would take the 30 most similar item, calculate the popularity score and then return the top 10

        NOTE Warning, for now, we do not have any genre linked to books !

        Returns:
            DataFrame: dataframe of track data
        """
        self.df = pd.read_sql_query(
            'SELECT b.content_id, b.title, b.author, b.year_of_publication, b.publisher FROM "book" AS b', con=db.engine)

        # Reduce memory
        self.reduce_memory()

        return self.df

    def prepare_sim(self):
        """Prepare book data for content similarity process

        Returns:
            DataFrame: result dataframe
        """
        book_df = self.get_with_genres()
        # remove '0' from year
        book_df["year_of_publication"] = book_df["year_of_publication"].astype(
            str)
        book_df["year_of_publication"] = book_df["year_of_publication"].replace(
            '0', '')

        # Replace NaN with an empty string
        features = ['title', 'author', 'year_of_publication', 'publisher']
        for feature in features:
            book_df[feature] = book_df[feature].fillna('')

        # Clean and homogenise data
        features = ['title', 'author', 'publisher']
        for feature in features:
            book_df[feature] = book_df[feature].apply(clean_data)

        # Create a new soup feature
        book_df['soup'] = book_df.apply(
            lambda x: create_soup(x, features), axis=1)

        # Delete unused cols (feature)
        book_df = book_df.drop(
            ['title', 'author', 'year_of_publication', 'publisher'], 1)

        return book_df
