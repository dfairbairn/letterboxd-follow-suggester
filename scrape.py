import json
import logging
import sqlite3
import time
from scraping import members, user

logging.basicConfig()
logger = logging.getLogger(__name__)



from datetime import datetime
from dataclasses import dataclass
import hashlib
@dataclass
class RatingObject:
    """ A class for structuring film ratings data. TODO: Do validation here """
    def __init__(self, film_title, film_url, film_id, film_rating, user):
        # Database ID entries will be unique to a user/film_id combo.
        # **This implies we only count a single rating of a film by a user **
        self.id = hashlib.md5( bytes(film_id, "UTF-8") + bytes(user, "UTF-8") ).hexdigest()

        self.last_updated = datetime.utcnow()
        self.film_title = film_title
        self.film_url = film_url
        self.film_id = film_id
        self.film_rating = film_rating
        self.user = user


class Scraper:
    def __init__(self, scraping_function, *args, **kwargs):
        self.scraping_function = scraping_function
        self._args = args
        self._kwargs = kwargs
        self._results = self.results = []

    def scrape(self):
        self._results = self.scraping_function(*self._args, **self._kwargs)


class RatingScraper(Scraper):
    def structure_results(self):
        """
        Take unstructured results self._results (from self.scraping_function), add RatingObject structuring
         Precondition 1: self._results has output from self.scraping_function (we won't do much productive here if not)
         Precondition 2: self.results is empty (we're about to overwrite it if not)
         Assumption: scraping_function outputs those results in the same order as it did originally
        """
        self.results = []
        for rating in self._results:
            # Example entry in _results: (film_title_unreliable, film_id, film_url_pattern, rating, user )
            ro = RatingObject(
                film_title=rating[0], film_id=rating[1], film_url=rating[2], film_rating=rating[3], user=rating[4])
            self.results.append(ro)
        return self.results

    @staticmethod
    def translate_stars(rating):
        if rating == "" or rating == "NR":
            # coerce all "" values into NR, b/c accidental ratings that get regraded to "" should usually be considered NR
            return None
        elif "★" in rating:
            return 0.5 * rating.count("★")
        else:
            print(f"[Error] weird value: {rating}")
            return None


class ParsingStorage:
    def __init__(self):
        self.connection = sqlite3.connect('parse_store.db')
        # self.connection = sqlite3.connect(':memory:')
        self.cursor = self.connection.cursor()
        # if tables don't exist, create them
        self.create_parsing_table()

    def create_parsing_table(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, "
                            "name TEXT, url TEXT, last_updated TEXT)")

        self.cursor.execute("CREATE TABLE IF NOT EXISTS movies(id INTEGER PRIMARY KEY, name TEXT, url TEXT, "
                            "last_updated TEXT)")

        # TODO: Decide on whether to include reviews in this table, or store those separately?
        self.cursor.execute("CREATE TABLE IF NOT EXISTS ratings(id TEXT PRIMARY KEY, user TEXT, film_title TEXT,"
                            "film_url TEXT, film_id TEXT, film_rating REAL, last_updated TEXT)")

    def insert_rating(self, ro: RatingObject):
        self.cursor.execute("INSERT INTO ratings (id, user, film_title, film_url, film_id, film_rating, last_updated) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (ro.id, ro.user, ro.film_title, ro.film_url, ro.film_id, ro.film_rating, ro.last_updated))

        self.connection.commit()




def main():
    # Do script stuff

    # 0. Set up DB
    db = ParsingStorage()

    # 1. Get active member list
    # N = 30
    # member_list = members.top_users(N)
    # with open(f'top_2023_members_{N}.json', 'w') as f:
    #      json.dump(member_list, f)

    # 2. *Slowly*, grab all movie ratings for each user
    # user_ratings = {}
    # for m in member_list:
    #     time.sleep(1)
    #     user_info = user.User(m)
    #     ratings_for_user = user.user_films_rated(user_info)
    #     user_ratings[m] = ratings_for_user
    # 2b. Database testing hack:
    user_ratings = {'davidteef': []}
    with open('scraping/davidteef_ratings.json', 'r') as f:
        user_ratings['davidteef'] = json.load(f)

    # 3. Output scraped results: Fill DB up with scraped data
    # let's try storing our ratings in the DB table then.


if __name__ == "__main__":
    # main()
    db = ParsingStorage()

    # Skip the scraping step here to test data structuring/database insertion
    with open('scraping/davidteef_ratings.json', 'r') as f:
        ratings = json.load(f)

    # Test adding structuring to the ratings scrape
    rs = RatingScraper(user.user_films_rated)
    rs._results = ratings
    structured_ratings = rs.structure_results()

    # Test insertion into the DB
    for r in structured_ratings:
        db.insert_rating(r)
