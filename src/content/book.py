from src.utils import db
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
    def get(cls):
        pass

    @staticmethod
    def get_with_genres(book_df):
        pass
