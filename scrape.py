import argparse
import sqlite3
from scraping import members, user
from datetime import datetime, timedelta
import hashlib
import time
import random


BACKOFF_TIME_BASE = 10.0
SCRAPE_DB = "scrape_store.db"


class DbObject:
    def __repr__(self):
        return str(self.__dict__.items())

    def __str__(self):
        return str(self.__dict__.values())


class RatingObject(DbObject):
    """ A class for structuring film ratings data. TODO: Do validation here """
    def __init__(self, film_title, film_url, film_id, film_rating, user):
        # Database ID entries will be unique to a user/film_id combo.
        # **This implies we only count a single rating of a film by a user **

        self.last_updated: int = int(datetime.utcnow().timestamp())
        self.film_title: str = film_title
        self.film_url: str = film_url
        self.film_id: int = int(film_id)
        self.film_rating: float = film_rating
        self.user: int = user
        self.id: int = int(hashlib.md5(
                                  bytes(film_id, "UTF-8") + bytes(user, "UTF-8") + bytes(str(self.last_updated),
                                                                                         "UTF-8")
                                 ).hexdigest()[:15], 16)


class ScrapedUserObject(DbObject):
    """ A class for structuring scrapes of Letterboxd users """
    def __init__(self, user):
        self.user: int = user
        # Initialize the member at 0 since they haven't been scraped yet
        self.last_updated: int = 0  # int(datetime.utcnow().timestamp())
        self.id: int = int(hashlib.md5(
                                       bytes(user, "UTF-8") + bytes(str(self.last_updated), "UTF-8")
                                      ).hexdigest()[:15], 16)


class Scraper:
    def __init__(self, scraping_function, *args, **kwargs):
        self.scraping_function = scraping_function
        self._args = args
        self._kwargs = kwargs
        self._results = self.results = []

    def scrape(self):
        print(f"[Debug] Executing function {self.scraping_function}")
        self._results = self.scraping_function(*self._args, **self._kwargs)


class UsersScraper(Scraper):
    def structure_results(self):
        """
        Fit the user list into ScrapedUserObjects
        :return:
        """
        self.results = []
        print(f"  [Debug] Prettifying top users results")
        for user in self._results:
            self.results.append(ScrapedUserObject(user))
        return self.results


class RatingScraper(Scraper):
    def structure_results(self):
        """
        Take unstructured results self._results (from self.scraping_function), add RatingObject structuring
         Precondition 1: self._results has output from self.scraping_function (we won't do much productive here if not)
         Precondition 2: self.results is empty (we're about to overwrite it if not)
         Assumption: scraping_function outputs those results in the same order as it did originally
        """
        print(f"  [Debug] Prettifying user film ratings results for {user=}")
        self.results = []
        for rating in self._results:
            # Example entry in _results: (film_title_unreliable, film_id, film_url_pattern, rating, user )
            rating_val = RatingScraper.translate_stars(rating[3])
            # print(f"{rating} --------> {rating_val}")
            ro = RatingObject(
                film_title=rating[0], film_id=rating[1], film_url=rating[2], film_rating=rating_val, user=rating[4])
            self.results.append(ro)
        return self.results

    @staticmethod
    def translate_stars(rating):
        rating_val = 0
        if "½" in rating:
            rating_val += 0.5
            print(f"'½' found in {rating}")
        if "★" in rating:
            rating_val += rating.count("★")

        if rating_val == 0:
            print(f"rating with no 1/2 or stars: {rating}")

        if rating == "" or rating == "NR":
            # coerce all "" values into NR, b/c accidental ratings that get regraded to "" should usually be considered NR
            rating_val = None

        return rating_val


class ParsingStorage:
    def __init__(self):
        self.connection = sqlite3.connect(SCRAPE_DB)
        # self.connection = sqlite3.connect(':memory:')
        self.cursor = self.connection.cursor()
        # if tables don't exist, create them
        self.create_parsing_table()

    def create_parsing_table(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, "
                            "user TEXT, last_updated INTEGER)")

        # TODO: Decide on whether to include reviews in this table, or store those separately?
        self.cursor.execute("CREATE TABLE IF NOT EXISTS ratings(id INTEGER PRIMARY KEY, user TEXT, film_title TEXT,"
                            "film_url TEXT, film_id INTEGER, film_rating REAL, last_updated INTEGER)")

    def insert_rating(self, ro: RatingObject):
        self.cursor.execute("INSERT OR IGNORE INTO ratings (id, user, film_title, film_url, film_id, film_rating, last_updated) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (ro.id, ro.user, ro.film_title, ro.film_url, ro.film_id, ro.film_rating, ro.last_updated))

        self.connection.commit()

    def insert_member(self, suo: ScrapedUserObject):
        self.cursor.execute("INSERT OR IGNORE INTO users (id, user, last_updated) VALUES (?, ?, ?)",
                            (suo.id, suo.user, suo.last_updated))

    def get_stale_users(self):
        stale_timestamp = int((datetime.utcnow() - timedelta(7)).timestamp())
        print(f"{stale_timestamp=}")
        self.cursor.execute(f"SELECT user FROM users where last_updated < {stale_timestamp}")
        users = self.cursor.fetchall()
        users_strs = [u[0] for u in users]
        print(f"  [Info] Found {len(users_strs)} members in DB needing a film rating update")
        return users_strs

    def close(self):
        self.connection.close()


def cli_get_top_members(get_top_members):
    print(f"[Info] Get Top {get_top_members} Members")

    db = ParsingStorage()

    us = UsersScraper(members.top_users, int(get_top_members))
    us.scrape()
    structured_users = us.structure_results()
    for su in structured_users:
        print(f"  {str(su)} ")
        db.insert_member(su)
    db.connection.commit()


def cli_user_film_ratings(user_film_ratings):
    print(f"[Info] User Film Ratings {user_film_ratings}")

    db = ParsingStorage()

    userinfo = user.User(user_film_ratings)
    rs = RatingScraper(user.user_films_rated, userinfo)
    rs.scrape()
    structured_ratings = rs.structure_results()

    # Test insertion into the DB
    for r in structured_ratings:
        print(f"  [Debug] {str(r)}")
        db.insert_rating(r)
    db.connection.commit()


def cli_update_film_ratings(max_users_to_update=30, report_stale_users_only=False):
    print(f"[Info] Check user updates")

    db = ParsingStorage()
    stale_user_list = db.get_stale_users()
    db.close()
    if not report_stale_users_only:
        for i, stale_user in enumerate(stale_user_list):
            print(f"  [{i}] - [Debug] Fetching film ratings for user '{stale_user}'...")
            if i >= max_users_to_update:
                break
            cli_user_film_ratings(stale_user)
            sleep_time = BACKOFF_TIME_BASE * (1 + random.random())
            time.sleep(sleep_time)
    else:
        for u in stale_user_list:
            print(u)

def main():
    parser = argparse.ArgumentParser("A scraper")
    parser.add_argument('--get-top-members', '-top', dest="get_top_members",
                        help="Scrape < N > most popular Letterboxd members of the last year"
                        " and insert into DB.")
    parser.add_argument('--user-film-ratings', '-ufr', dest="user_film_ratings",
                        help="Scrape all film ratings for < user >, insert into DB ")
    parser.add_argument('--users-to-update', '-utu', dest="users_to_update", action="store_true",
                        help="Flag denoting to list users whose film ratings have not been scraped in the last 7 days")
    parser.add_argument('--update-film-ratings', '-upd', dest="update_film_ratings", action="store_true",
                        help="Flag denoting to scrape film ratings for all users in DB not scraped in the last 7 days")
    args = parser.parse_args()

    print(f"{args=}")
    if args.get_top_members:
        cli_get_top_members(args.get_top_members)

    elif args.user_film_ratings:
        cli_user_film_ratings(args.user_film_ratings)

    elif args.users_to_update:
        cli_update_film_ratings(report_stale_users_only=True)

    elif args.update_film_ratings:
        cli_update_film_ratings()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()

    # db = ParsingStorage()
    #
    # # Skip the scraping step here to test data structuring/database insertion
    # with open('scraping/davidteef_ratings.json', 'r') as f:
    #     ratings = json.load(f)
    #
    # # Test adding structuring to the ratings scrape
    # rs = RatingScraper(user.user_films_rated)
    # rs._results = ratings
    # structured_ratings = rs.structure_results()
    #
    # # Test insertion into the DB
    # for r in structured_ratings:
    #     db.insert_rating(r)
