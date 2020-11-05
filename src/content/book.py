from src.utils import db, clean_data, create_soup
import pandas as pd
import numpy as np


class Book:
    __meta_cols__ = ["user_id", "isbn",
                     "rating", "purchase", "review_see_count"]

    @staticmethod
    def reduce_memory(book_df):
        cols = list(book_df.columns)

        # Reduce memory
        if "year_of_publication" in cols:
            book_df["year_of_publication"] = book_df["year_of_publication"].astype(
                "uint16")
        if "rating" in cols:
            book_df["rating"] = book_df["rating"].astype("float32")
        if "rating_count" in cols:
            book_df["rating_count"] = book_df["rating_count"].astype("uint32")
        if "popularity_score" in cols:
            book_df["popularity_score"] = book_df["popularity_score"].astype(
                "float32")

        return book_df

    @classmethod
    def get_meta(cls, cols=None):
        pass

    @classmethod
    def get_ratings(cls):
        """Get all books and their metadata

        Returns:
            DataFrame: book dataframe
        """
        book_df = pd.read_sql_query(
            'SELECT isbn, rating, rating_count FROM "book"', con=db.engine)

        # Reduce memory
        book_df = cls.reduce_memory(book_df)

        return book_df

    @classmethod
    def get_books(cls):
        book_df = pd.read_sql_query('SELECT * FROM "book"', con=db.engine)

        # Reduce memory
        book_df = cls.reduce_memory(book_df)

        return book_df

    @classmethod
    def get_with_genres(cls):
        """Get book

        NOTE can add 't.rating' and 't.rating_count' column if we introduce popularity filter to content-based engine
            example: this recommender would take the 30 most similar item, calculate the popularity score and then return the top 10

        NOTE Warning, for now, we do not have any genre linked to books !

        Returns:
            DataFrame: dataframe of track data
        """
        book_df = pd.read_sql_query(
            'SELECT b.isbn, b.title, b.author, b.year_of_publication, b.publisher FROM "book" AS b', con=db.engine)

        # Reduce memory
        book_df = cls.reduce_memory(book_df)

        return book_df

    @staticmethod
    def prepare_sim(book_df):
        """Prepare book data for content similarity process

        Args:
            book_df (DataFrame): Book dataframe

        Returns:
            DataFrame: result dataframe
        """
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
