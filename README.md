# Letterboxd Follow Suggester

Enter your letterboxd username, receive a tailored suggestion for a Letterboxd user to follow!



## Scraping Subsystem Usage

Updating user list:
```bash
scrape.py --get-top-members <N>
# scrapes members lists by popularity, up to N
# populates DB with members list, profile URLs, last_updated stat for scrapes

```


Getting film ratings for a user:
```bash
scrape.py --user-film-ratings <user>
# scrapes all films watched by user, including unrated or blank (more expansive than diary entries)
# populates into the DB's ratings table (film_title_approx, film_url, film_id, film_rating, user, hash(film_id, user, last_updated))

```


Updating all film ratings:
```bash
scrape.py --update-film-ratings [7d]
# triggers --user-film-ratings job for all users in DB users table not updated in >= 7d

```
