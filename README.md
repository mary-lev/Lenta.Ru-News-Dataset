# Lenta.Ru news corpus

Russian news articles from [lenta.ru](https://lenta.ru) and a script to download them.

## Datasets
| Dates | Articles | Download |
|---|---|---|
| 30.08.1999 – 14.12.2019 | 800K+ | [Kaggle](https://www.kaggle.com/yutkin/corpus-of-russian-news-articles-from-lenta/), [GitHub](https://github.com/yutkin/Lenta.Ru-News-Dataset/releases) (`bzip2 -d lenta-ru-news.csv.bz2`) |
| 12/2019 – 12/2023 | ~500K | [Kaggle](https://www.kaggle.com/datasets/marialevchenko/news-dataset-from-lenta-ru-2019-2023) |

Columns: `url`, `title`, `text`, `topic` (section, e.g. «Мир»), `tags` (subsection, e.g. «Политика»), `date` (`YYYY-MM-DD`, from the URL).

**Known issue:** in the 2019–2023 Kaggle version of January 2024, `url` and `date` do not match the article (a scraper bug, fixed in the script in November 2026). `title`, `text`, `topic` and `tags` are correct.

## Download script
Python 3.9+, `aiohttp`, `beautifulsoup4`.

```
python download_lenta.py --from-date 01.01.2024 --to-date 31.12.2024 --outfile lenta_2024.csv
```

- `--from-date` (default `30.08.1999`), `--to-date` (default: today), `--outfile` (default `lenta-ru-news.csv`).
- Running again with the same `--outfile` resumes from the last saved date; saved articles are not downloaded twice.
- Articles that fail after 3 retries are logged as `Cannot fetch <url>`.
