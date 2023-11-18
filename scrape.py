import argparse
from scraping import members, user
from datetime import datetime
import time
import random
from scraping.scraper import UsersScraper, RatingScraper
from utils import ParsingStorage


BACKOFF_TIME_BASE = 4.0


def cli_get_top_members(get_top_members):
    print(f"[Info] Get Top {get_top_members} Members")

    db = ParsingStorage()
    us = UsersScraper(members.top_users, int(get_top_members))
    us.scrape()
    structured_users = us.structure_results()
    for su in structured_users:
        print(f"  {str(su)} ")
        db.insert_member(su)


def cli_user_film_ratings(user_film_ratings):
    print(f"[Info] User Film Ratings {user_film_ratings}")

    db = ParsingStorage()

    userinfo = user.User(user_film_ratings)
    rs = RatingScraper(user.user_films_rated, userinfo)
    rs.scrape()
    structured_ratings = rs.structure_results()

    # Test insertion into the DB
    for r in structured_ratings:
        # print(f"  [Debug] {str(r)}")
        db.insert_rating(r)

    db.refresh_user(user_film_ratings)


def cli_update_film_ratings(max_users_to_update=50, report_stale_users_only=False):
    print(f"[Info] Check user updates")

    db = ParsingStorage()
    stale_user_list = db.get_stale_users()
    db.close()
    if not report_stale_users_only:
        for i, stale_user in enumerate(stale_user_list):
            if i >= max_users_to_update:
                break
            print(f"  [{i}] - [Debug] Fetching film ratings for user '{stale_user}'...")
            cli_user_film_ratings(stale_user)
            sleep_time = BACKOFF_TIME_BASE * (1 + random.random())
            time.sleep(sleep_time)
    else:
        for u in stale_user_list:
            print(u)


def cli_refresh_last_updated():
    """ Helper method if you've got film ratings data in the DB that you don't think needs to be updated"""
    print(f"[Info] Setting")
    db = ParsingStorage()

    # TODO: move into the ParsingStorage object
    db.cursor.execute("SELECT DISTINCT user from ratings")
    users_to_refresh = [entry[0] for entry in db.cursor.fetchall()]
    print(f"{users_to_refresh=}")

    for user in users_to_refresh:
        last_updated = int(datetime.utcnow().timestamp())
        db.cursor.execute(f"UPDATE users SET last_updated = {last_updated} WHERE user = '{user}';")
        print(f"  Updated to {last_updated}")
    db.connection.commit()


def main():
    parser = argparse.ArgumentParser("A scraper")
    parser.add_argument('--get-top-members', '-top', dest="get_top_members",
                        help="Scrape < N > most popular Letterboxd members of the last year"
                        " and insert into DB.")
    parser.add_argument('--user-film-ratings', '-ufr', dest="user_film_ratings",
                        help="Scrape all film ratings for < user >, insert into DB ")
    parser.add_argument('--update-film-ratings', '-upd', dest="update_film_ratings",
                        help="Scrape film ratings for < N > users in the DB needing updates (i.e. older than 7 days)")

    parser.add_argument('--users-to-update', '-utu', dest="users_to_update", action="store_true",
                        help="Flag denoting to list users whose film ratings have not been scraped in the last 7 days")
    parser.add_argument('--refresh-last-updated', '-r', dest="refresh_last_updated", action="store_true",
                        help="Update the 'last_updated' column in the DB for all users with film ratings")
    args = parser.parse_args()

    print(f"{args=}")
    if args.get_top_members:
        cli_get_top_members(args.get_top_members)

    elif args.user_film_ratings:
        cli_user_film_ratings(args.user_film_ratings)

    elif args.users_to_update:
        cli_update_film_ratings(report_stale_users_only=True)

    elif args.update_film_ratings:
        cli_update_film_ratings(max_users_to_update=int(args.update_film_ratings))

    elif args.refresh_last_updated:
        cli_refresh_last_updated()

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
