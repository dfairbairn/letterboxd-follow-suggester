import argparse
import json
import logging
import sqlite3
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

        self.last_updated = datetime.utcnow()
        self.film_title = film_title
        self.film_url = film_url
        self.film_id = film_id
        self.film_rating = film_rating
        self.user = user
        self.id = hashlib.md5( bytes(film_id, "UTF-8") + bytes(user, "UTF-8") + bytes(str(self.last_updated), "UTF-8") ).hexdigest()


@dataclass
class ScrapedUserObject:
    """ A class for structuring scrapes of Letterboxd users """
    def __init__(self, user):
        self.user = user
        self.last_updated = datetime.utcnow()
        self.id = hashlib.md5( bytes(user, "UTF-8") + bytes(str(self.last_updated), "UTF-8") )


class Scraper:
    def __init__(self, scraping_function, *args, **kwargs):
        self.scraping_function = scraping_function
        self._args = args
        self._kwargs = kwargs
        self._results = self.results = []

    def scrape(self):
        self._results = self.scraping_function(*self._args, **self._kwargs)


class UsersScraper(Scraper):
    def structure_results(self):
        """
        Fit the user list into ScrapedUserObjects
        :return:
        """
        self.results = []
        for rating in self._results:
            self.results.append(ScrapedUserObject(rating))
        return self.results


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
            rating_val = RatingScraper.translate_stars(rating[3])
            logger.info(f"{rating} --------> {rating_val}")
            ro = RatingObject(
                film_title=rating[0], film_id=rating[1], film_url=rating[2], film_rating=rating_val, user=rating[4])
            self.results.append(ro)
        return self.results

    @staticmethod
    def translate_stars(rating):

        rating_val = 0
        if "½" in rating:
            rating_val += 0.5
            logger.info(f"'½' found in {rating}")
        if "★" in rating:
            rating_val += rating.count("★")

        if rating_val == 0:
            logger.info(f"rating with no 1/2 or stars: {rating}")

        if rating == "" or rating == "NR":
            # coerce all "" values into NR, b/c accidental ratings that get regraded to "" should usually be considered NR
            rating_val = None

        return rating_val


class ParsingStorage:
    def __init__(self):
        self.connection = sqlite3.connect('parse_store.db')
        # self.connection = sqlite3.connect(':memory:')
        self.cursor = self.connection.cursor()
        # if tables don't exist, create them
        self.create_parsing_table()

    def create_parsing_table(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS users(id TEXT PRIMARY KEY, "
                            "user TEXT, last_updated TEXT)")

        # TODO: Decide on whether to include reviews in this table, or store those separately?
        self.cursor.execute("CREATE TABLE IF NOT EXISTS ratings(id TEXT PRIMARY KEY, user TEXT, film_title TEXT,"
                            "film_url TEXT, film_id TEXT, film_rating REAL, last_updated TEXT)")

    def insert_rating(self, ro: RatingObject):
        self.cursor.execute("INSERT INTO ratings (id, user, film_title, film_url, film_id, film_rating, last_updated) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (ro.id, ro.user, ro.film_title, ro.film_url, ro.film_id, ro.film_rating, ro.last_updated))

        self.connection.commit()

    def insert_member(self, suo: ScrapedUserObject):
        self.cursor.execute("INSERT INTO users (id, user, last_updated) VALUES (?, ?, ?)",
                            (suo.id, suo.user, suo.last_updated))

    def close(self):
        self.connection.close()


def main():
    parser = argparse.ArgumentParser("A scraper")
    parser.add_argument('--get-top-members', dest="n_top_members",
                        help="Scrape < N > most popular Letterboxd members of the last year"
                        " and insert into DB.")
    parser.add_argument('--user-film-ratings', dest="user",
                        help="Scrape all film ratings for < user >, insert into DB ")
    parser.add_argument('--update-film-ratings', dest="update", action="store_true",
                        help="Flag denoting to scrape film ratings for all users in DB not scraped in the last 7 days")
    args = parser.parse_args()

    logger.info(f"{args=}")
    if args.n_top_members:
        pass

    elif args.user:
        pass

    if args.update:
        pass


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
