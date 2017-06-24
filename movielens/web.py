from database import MovieLensDB

import logging

import tornado.ioloop
import tornado.web
import os

PORT = 8880
DATA_DIR = 'assets/data'
DB_FILE = os.path.join(DATA_DIR, 'movielens.db')

class MainHandler(tornado.web.RequestHandler):
    #Handles requests for the main page
    
    def initialize(self, db):
        self.db = db
        
    def get(self):
        genre_row = []
        for g in self.db.list_genres():
            genre_row.append(g['genre'])

        self.render('search_page.html', genre_row = genre_row)

class TitleSearchHandler(tornado.web.RequestHandler):
    #Handles title search requests
    
    def initialize(self, db):
        self.db = db
    
    def get(self):
        title = self.get_argument("query")
        result = self.db.search_title('%{}%'.format(title))
        self.render('movie_index.html', result = result)


class GenreSearchHandler(tornado.web.RequestHandler):
    #Handles genre search requests

    def initialize(self, db):
        self.db = db
    
    def get(self):
        genre = self.get_argument("query")
        result = self.db.search_genre(genre)
        self.render('movie_index.html', result = result)

class DetailHandler(tornado.web.RequestHandler):
    #Handles details for a single movie
    def initialize(self, db):
        self.db = db
    
    def get(self, movie_id):
        detail = self.db.movie_detail(movie_id)
        genre = self.db.get_genres(movie_id)
        rating = self.db.get_rating(movie_id)
        imdb = self.db.imdb_data(detail['imdb_id'])

        self.render('movie_detail.html', detail = detail, genre = genre, rating = rating, imdb = imdb)
class RatingHandler(tornado.web.RequestHandler):
    #Handles requests submitting new ratings
    
    def initialize(self, db):
        self.db = db
        
    def get(self):
        rating = self.get_argument("submit_rating")
        movie_id = self.get_argument("movie_id")

        setrating = self.db.set_rating(movie_id, rating)
        self.redirect('/detail/{}'.format(movie_id))

if __name__ == '__main__':
    db = MovieLensDB()
    
    app = tornado.web.Application([
            
            (r'/', MainHandler, {'db': db}),
            (r'/title', TitleSearchHandler, {'db': db}),
            (r'/genre', GenreSearchHandler, {'db': db}),
            (r'/detail/(\d+)', DetailHandler, {'db': db}),
            (r'/rating', RatingHandler, {'db': db}),
            (r'/css/(.*)', tornado.web.StaticFileHandler, {'path':'assets/css'})
        ],
        template_path = 'assets/templates',
        debug = True
    )

    app.listen(PORT)
    tornado.ioloop.IOLoop.current().start()
    
