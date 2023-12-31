"""
Adapted from https://github.com/nmcassa/letterboxdpy
"""
import re
from json import JSONEncoder
from scraping.base import Base


class List(Base):
    def __init__(self, author: str, title: str) -> None:
        if not re.match("^[A-Za-z0-9_]*$", author):
            raise Exception("Invalid author")

        self.title = title.replace(' ', '-').lower()
        self.author = author.lower()
        self.url = "https://letterboxd.com/" + self.author +"/list/" + self.title + "/"

        page = self.get_parsed_page(self.url)
    
        self.description(page)
        self.film_count(self.url)

    def list_title(self, page: None) -> str:
        data = page.find("meta", attrs={'property': 'og:title'})
        return data['content']

    def author(self, page: None) -> str:
        data = page.find("span", attrs={'itemprop': 'name'})
        return data.text

    def description(self, page: None) -> str:
        try:
            data = page.find_all("meta", attrs={'property': 'og:description'})
            self.description = data[0]['content']
        except:
            return None

    def film_count(self, url: str) -> int: #and movie_list!!
        prev = count = 0
        curr = 1
        movie_list = []
        while prev != curr:
            count += 1
            prev = len(movie_list)
            page = self.get_parsed_page(url + "page/" + str(count) + "/")

            img = page.find_all("img", {"class": ["image"], })

            for item in img:
                movie_url = item.parent['data-film-slug']
                movie_list.append((item['alt'], movie_url))
                
            curr = len(movie_list)

        self.filmCount = curr
        self.movies = movie_list

        if self.filmCount == 0:
            raise Exception("No list exists")


def list_tags(list: List) -> list:
    if type(list) != List:
        raise Exception("Improper parameter")

    ret = []

    data = list.get_parsed_page(list.url)
    data = data.find("ul", {"class": ["tags"], })
    data = data.findChildren("a")

    for item in data:
        ret.append(item.text)

    return ret


class Encoder(JSONEncoder):
    def default(self, o):
        return o.__dict__


if __name__ == "__main__":
    list = List("eddiebergman", "movie-references-made-in-nbcs-community")
    print(list_tags(list))
