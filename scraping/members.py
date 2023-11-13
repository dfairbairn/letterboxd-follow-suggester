import json
from typing import List
from base import Base


MEMBERS_YEAR_TOP = "https://letterboxd.com/members/popular/this/year/"


class MemberListing(Base):

    def __init__(self, url=""):
        self.listing_base = url


def top_users(n: int) -> List:
    ml = MemberListing(url=MEMBERS_YEAR_TOP)
    page = ml.get_parsed_page(ml.listing_base)

    # returns all movies
    prev = -1
    count = 1
    curr = 0
    member_list = []

    while prev != curr and curr < n:
        count += 1
        prev = len(member_list)
        popular_members_table = page.find_all('table', {"class": ["person-table"], })[0]
        avatars = popular_members_table.find_all("a", {"class": ["avatar -a40"], })

        for avatar in avatars:
            acct_path = avatar['href']
            acct = acct_path.replace('/', '')
            member_list.append(acct)

        curr = len(member_list)
        page = ml.get_parsed_page(ml.listing_base + '/page/' + str(count) + "/")

    return member_list[:n]


if __name__=="__main__":
    N = 100
    member_list = top_users(N)

    with open(f'top_2023_members_{len(member_list)}.json', 'w') as f:
        json.dump(member_list, f)