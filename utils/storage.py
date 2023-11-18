from datetime import datetime, timedelta
import sqlite3
from scraping.scraper import RatingObject, ScrapedUserObject


SCRAPE_DB = "scrape_store.db"


class ParsingStorage:
    def __init__(self, in_memory=False):
        if in_memory:
            self.connection = sqlite3.connect(':memory:')
        else:
            self.connection = sqlite3.connect(SCRAPE_DB)

        self.cursor = self.connection.cursor()
        # if tables don't exist, create them
        self.create_parsing_table()

    def create_parsing_table(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, "
                            "user TEXT, last_updated INTEGER)")

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

        self.connection.commit()

    def get_stale_users(self):
        """ Returns list of all users not updated in the last 7 days """
        stale_timestamp = int((datetime.utcnow() - timedelta(7)).timestamp())
        print(f"{stale_timestamp=}")
        self.cursor.execute(f"SELECT user FROM users where last_updated < {stale_timestamp}")
        users = self.cursor.fetchall()
        users_strs = [u[0] for u in users]
        print(f"  [Info] Found {len(users_strs)} members in DB needing a film rating update")
        return users_strs

    def refresh_user(self, username):
        # Update the users table with the new 'last_updated' value
        last_updated = int(datetime.utcnow().timestamp())
        self.cursor.execute(f"UPDATE users SET last_updated = {last_updated} WHERE user = '{username}';")
        self.connection.commit()

    def close(self):
        self.connection.close()


if __name__ == "__main__":
    db = ParsingStorage(in_memory=True)
