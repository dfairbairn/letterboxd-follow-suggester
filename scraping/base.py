from bs4 import BeautifulSoup
import json
import re
import requests


class Base:

    def self_check_value(self, value):
        """ Check to ensure no particularly exotic characters are present """
        if not re.match("^[A-Za-z0-9_()-]*$", value):
            raise Exception(f"Invalid {self.__class__.__name__}")

    def __str__(self):
        return self.jsonify()

    def jsonify(self) -> str:
        return json.dumps(self, indent=4, cls=Encoder)

    @staticmethod
    def get_parsed_page(url: str) -> BeautifulSoup:

        # From letterboxdpy on Github:
        # This fixes a blocked by cloudflare error i've encountered
        headers = {
            "referer": "https://letterboxd.com",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }

        return BeautifulSoup(requests.get(url, headers=headers).text, "lxml")


class Encoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__