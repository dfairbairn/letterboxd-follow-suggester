from datetime import datetime
import hashlib


class DbObject:
    def __repr__(self):
        return str(self.__dict__.items())

    def __str__(self):
        return str(self.__dict__.values())


class RatingObject(DbObject):
    """ A class for structuring film ratings data. """
    def __init__(self, film_title, film_url, film_id, film_rating, username):
        # Database ID entries will be unique to a user/film_id combo.
        # **This implies we only count a single rating of a film by a user **

        self.last_updated: int = int(datetime.utcnow().timestamp())
        self.film_title: str = film_title
        self.film_url: str = film_url
        self.film_id: int = int(film_id)
        self.film_rating: float = film_rating
        self.user: int = username
        self.id: int = int(hashlib.md5(
                                  bytes(film_id, "UTF-8") + bytes(username, "UTF-8") + bytes(str(self.last_updated),
                                                                                         "UTF-8")
                                 ).hexdigest()[:15], 16)


class ScrapedUserObject(DbObject):
    """ A class for structuring scrapes of Letterboxd users """
    def __init__(self, username):
        self.user: str = username
        # Initialize the member at 0 since they haven't been scraped yet
        self.last_updated: int = 0  # int(datetime.utcnow().timestamp())
        self.id: int = int(hashlib.md5(
                                       bytes(username, "UTF-8") + bytes(str(self.last_updated), "UTF-8")
                                      ).hexdigest()[:15], 16)


"""  =======================================================================  """


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
        self.results = []
        if not self._results and len(self._results) > 0 and len(self._results[0]) != 5:
            print("[Error] Problem with scraped ratings")
        else:
            print(f"[Debug] Structuring scraped ratings for '{self._args}', likely user '{self._results[0][4]}'")
            for rating in self._results:
                # Example entry in _results: (film_title_unreliable, film_id, film_url_pattern, rating, user )
                rating_val = RatingScraper.translate_stars(rating[3])
                # print(f"{rating} --------> {rating_val}")
                ro = RatingObject(
                    film_title=rating[0],
                    film_id=rating[1],
                    film_url=rating[2],
                    film_rating=rating_val,
                    username=rating[4])
                self.results.append(ro)
        return self.results

    @staticmethod
    def translate_stars(rating):
        rating_val = 0
        if "½" in rating:
            rating_val += 0.5
            # print(f"'½' found in {rating}")
        if "★" in rating:
            rating_val += rating.count("★")

        if rating_val == 0:
            # print(f"rating with no 1/2 or stars: {rating}")
            pass
        if rating == "" or rating == "NR":
            # coerce all "" values into NR, b/c accidental ratings that get regraded to "" should usually be considered NR
            rating_val = None

        return rating_val
