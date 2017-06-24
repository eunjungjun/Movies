#!/usr/bin/env python

import sqlite3
import os
import requests

class MovieLensDB(object):
    #A connection to the MovieLens database

    DATA_DIR = 'assets/data'
    DB_FILE = os.path.join(DATA_DIR, 'movielens.db')
    TABLES = ['Movie', 'GenreTag', 'Rating']
    
    
    def __init__(self, db_file=DB_FILE, data_dir=DATA_DIR):
        #Initialize a new database connection

        self.db_file  = db_file
        self.data_dir = data_dir
        self.conn     = sqlite3.connect(db_file)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute('PRAGMA foreign_keys = ON')
       
        self._create_tables()
        self._populate_tables()
        
    
    def _create_tables(self):
        #Execute the SQL commands in sql_file to create tables

        cur = self.conn.cursor()

        cur.execute('''
        DROP TABLE IF EXISTS Rating
        ''')

        cur.execute('''
        DROP TABLE IF EXISTS GenreTag
        ''')

        cur.execute('''
        DROP TABLE IF EXISTS Movie
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS Movie (
                movie_id INTEGER PRIMARY KEY,
                title    TEXT,
                year     INTEGER,
                imdb_id  TEXT
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS GenreTag (
                gtag_id  INTEGER PRIMARY KEY,
                movie_id INTEGER,
                genre    TEXT,
                FOREIGN KEY (movie_id) REFERENCES Movie(movie_id)
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS Rating (
                rating_id INTEGER PRIMARY KEY,
                movie_id  INTEGER,
                rating    INTEGER,
                FOREIGN KEY (movie_id) REFERENCES Movie(movie_id)
            )
        ''')
    
    
    
    def _populate_tables(self):
        #Populate the tables using local TSV data

        movie_file  = os.path.join(self.data_dir, 'movies.tsv')
        genre_file  = os.path.join(self.data_dir, 'genres.tsv')
        rating_file = os.path.join(self.data_dir, 'ratings.tsv')

        cur = self.conn.cursor()

        insert_sql = '''
        INSERT INTO Movie VALUES (?, ?, ?, ?)
        '''

        with open(movie_file) as fh:
            fh.readline()
            for line in fh:
                fields = line.strip().split('\t')

                if len(fields) == 4:
                    cur.execute(insert_sql, fields)
        
        insert_sql = '''
        INSERT INTO GenreTag VALUES (NULL, ?, ?)
        '''

        with open(genre_file) as fh:
            fh.readline()
            for line in fh:
                fields = line.strip().split('\t')

                if len(fields) == 2:
                    cur.execute(insert_sql, fields)

        insert_sql = '''
        INSERT INTO Rating VALUES (NULL, ?, ?)
        '''

        with open(rating_file) as fh:
            fh.readline()
            for line in fh:
                fields = line.strip().split('\t')

                if len(fields) == 2:
                    cur.execute(insert_sql, fields)

        self.conn.commit()
    
    def search_title(self, title):
        #Return a list of movies that match title

        cur = self.conn.cursor()

        result = []

        sql_query = '''
        SELECT 
            *
        FROM 
            Movie
        WHERE
            title LIKE ?
        '''
        
        cur.execute(sql_query, (title,))
        result = cur.fetchall()
        return result
    
    def search_genre(self, genre):
        #Return a list of movies tagged with a genre
        
        cur = self.conn.cursor()
        
        sql_query = '''
        SELECT 
            *
        FROM 
            Movie JOIN GenreTag
        USING 
            (movie_id)
        WHERE
            genre == ?
        '''
        
        cur.execute(sql_query, (genre,))
        result = cur.fetchall()
        return result
    
    def movie_detail(self, movie_id):
        #Return details for a single movie
        
        cur = self.conn.cursor()

        sql_query = '''
        SELECT
            *
        FROM
            Movie
        WHERE
            movie_id == ?
        '''

        cur.execute(sql_query, (movie_id,))
        result = cur.fetchone()
        return result

    def get_rating(self, movie_id):
        #return the average rating for a specific movie'''

        cur = self.conn.cursor()
        
        sql_query = '''
        SELECT
            round(avg(rating), 2) as rating,
            count(rating) as count
        FROM
            Rating
        WHERE
            movie_id == ?
        '''

        cur.execute(sql_query, (movie_id,))
        result = cur.fetchone()

        return result

    def get_genres(self, movie_id):
        #Return the list of genres for a specific movie'''
        
        cur = self.conn.cursor()

        sql_query = '''
        SELECT
            genre
        FROM
            GenreTag
        WHERE
            movie_id == ?
        '''

        cur.execute(sql_query, (movie_id,))
        result = cur.fetchall()
        return result
    
    def set_rating(self, movie_id, rating):
        #Add a user rating for a movie'''

        cur = self.conn.cursor()

        insert_sql = '''
        INSERT into Rating VALUES(
        NULL,
        ?,
        ?
        )
        '''
        if 1 <= rating <= 5:
            cur.execute(insert_sql, (movie_id, rating))
            self.conn.commit()
    
    def list_genres(self):
        #List all distinct genre tags in the database'''
        
        cur = self.conn.cursor()

        sql_query = '''
        SELECT
            genre
        FROM
            GenreTag
        '''

        cur.execute(sql_query)
        result = cur.fetchall()
        return sorted(set(result))
    
    def imdb_data(self, imdb_id):
        #Query the Open Movie Database for extra metadata
        
        URL = 'http://www.omdbapi.com/?i={}'.format(imdb_id)
        res = requests.get(URL)
        if res.ok:
            return res.json()
