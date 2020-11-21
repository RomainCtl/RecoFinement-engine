from src.utils import db, clean_data, create_soup
import pandas as pd
import numpy as np


class Book:
    __meta_cols__ = ["user_id", "isbn",
                     "rating", "purchase", "review_see_count"]

    id = "isbn"
    id_type = str
    tablename_recommended = "recommended_book"
    tablename_similars = "similars_book"
    tablename_media = "book"
    uppername = tablename_media.upper()

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
            book_df["rating_count"] = book_df["rating_count"].fillna(0)
            book_df["rating_count"] = book_df["rating_count"].astype("uint32")
        if "popularity_score" in cols:
            book_df["popularity_score"] = book_df["popularity_score"].astype(
                "float32")

        return book_df

    @classmethod
    def get_meta(cls, cols=None, user_id=None):
        """Get user metabook metadata

        Returns:
            DataFrame: pandas DataFrame
        """
        if cols is None:
            cols = cls.__meta_cols__
        assert all([x in cls.__meta_cols__ for x in cols])

        filt = ''
        if user_id is not None:
            filt = "WHERE user_id = '%s'" % user_id

        df = pd.read_sql_query('SELECT %s FROM "meta_user_book" %s' % (
            ', '.join(cols), filt), con=db.engine)

        # Reduce memory usage for ratings
        if 'user_id' in cols:
            df['user_id'] = df['user_id'].astype("uint32")
        if 'rating' in cols:
            df['rating'] = df['rating'].fillna(0)
            df['rating'] = df['rating'].astype("uint8")
        if 'purchase' in cols:
            df['purchase'] = df['purchase'].astype("uint16")
        if 'review_see_count' in cols:
            df['review_see_count'] = df['review_see_count'].astype("uint16")

        return df

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
    def get_similars(cls, isbn):
        """Get all similars content of a book

        Args:
            isbn (str): book unique id

        Returns:
            Dataframe: similars book dataframe
        """
        book_df = pd.read_sql_query(
            'SELECT sb.isbn0 as isbn, sb.isbn1 as similar_isbn, sb.similarity, b.popularity_score FROM "similars_book" AS sb INNER JOIN "book" AS b ON b.isbn = sb.isbn1 WHERE isbn0 = \'%s\'' % isbn, con=db.engine)

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
