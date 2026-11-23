import argparse
import asyncio
import csv
import logging
import os
import re
import sys
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from multiprocessing import cpu_count

import aiohttp
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s @ %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)
logger = logging.getLogger(name="LentaParser")

FIELDNAMES = ["url", "title", "text", "topic", "tags", "date"]


class LentaParser:

    # lxml is much faster but error prone
    default_parser = "html.parser"

    def __init__(self, *, max_workers: int, outfile_name: str, from_date: str, to_date: str = None):
        self._endpoint = "https://lenta.ru/news"

        self._sess = None
        self._connector = None

        self._executor = ProcessPoolExecutor(max_workers=max_workers)

        self._outfile_name = outfile_name
        self._outfile = None
        self._csv_writer = None
        self.timeouts = aiohttp.ClientTimeout(total=60, connect=60)

        self._n_downloaded = 0
        self._from_date = datetime.strptime(from_date, "%d.%m.%Y")
        self._to_date = datetime.strptime(to_date, "%d.%m.%Y") if to_date else datetime.today()

        # URLs already written (from a previous run or an earlier day page); never saved twice
        self._seen_urls = set()

        last_date = self._load_existing_output()
        if last_date is not None and last_date > self._from_date:
            logger.info(f"{outfile_name} already has news up to {last_date:%d.%m.%Y}, "
                        f"resuming from that date (already saved news are skipped)")
            self._from_date = last_date

    def _load_existing_output(self):
        """Remember URLs already in the output file and return its latest date (None for a new file)."""
        if not os.path.exists(self._outfile_name) or os.path.getsize(self._outfile_name) == 0:
            return None

        csv.field_size_limit(sys.maxsize)
        last_date = None
        with open(self._outfile_name, newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != FIELDNAMES:
                raise SystemExit(f"{self._outfile_name} has columns {reader.fieldnames}, expected {FIELDNAMES}. "
                                 f"Use another --outfile.")
            for row in reader:
                self._seen_urls.add(row["url"])
                if row["date"] and (last_date is None or row["date"] > last_date):
                    last_date = row["date"]

        return datetime.strptime(last_date, "%Y-%m-%d") if last_date else None

    @property
    def dates_countdown(self):
        date_start, date_end = self._from_date, self._to_date

        while date_start <= date_end:
            yield date_start.strftime("%Y/%m/%d")
            date_start += timedelta(days=1)

    @property
    def writer(self):
        if self._csv_writer is None:
            is_new_file = not os.path.exists(self._outfile_name) or os.path.getsize(self._outfile_name) == 0
            self._outfile = open(self._outfile_name, "a", 1)
            self._csv_writer = csv.DictWriter(self._outfile, fieldnames=FIELDNAMES)
            if is_new_file:
                self._csv_writer.writeheader()

        return self._csv_writer

    @property
    def session(self):
        if self._sess is None or self._sess.closed:

            self._connector = aiohttp.TCPConnector(
                use_dns_cache=True, ttl_dns_cache=60 * 60, limit=32
            )
            self._sess = aiohttp.ClientSession(
                connector=self._connector, timeout=self.timeouts
            )

        return self._sess

    async def fetch(self, url: str, attempts: int = 3):
        for attempt in range(1, attempts + 1):
            try:
                response = await self.session.get(url, allow_redirects=False)
                response.raise_for_status()
                return await response.text(encoding="utf-8")
            except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, asyncio.TimeoutError) as exc:
                # Dropped connections are transient: wait a bit and retry
                if attempt == attempts:
                    raise
                logger.warning(f"Retrying {url} ({attempt}/{attempts - 1}) after {type(exc).__name__}: {exc}")
                await asyncio.sleep(5 * attempt)

    @staticmethod
    def parse_article_html(html: str):
        doc_tree = BeautifulSoup(html, LentaParser.default_parser)
        tags = doc_tree.find("a", "rubric-header__link _active")
        tags = tags.get_text() if tags else None

        body = doc_tree.find("div", "topic-body__content")

        if not body:
            raise RuntimeError(f"Article body is not found")

        text = " ".join([p.get_text() for p in body.find_all("p", "topic-body__content-text")])

        topic = doc_tree.find("a", "topic-header__item topic-header__rubric")
        topic = topic.get_text() if topic else None

        title = doc_tree.find("h1", "topic-body__titles")
        title = title.get_text() if title else None

        if tags is None and topic:
            tags = LentaParser._tags_from_page_title(doc_tree, topic)

        return {"title": title, "text": text, "topic": topic, "tags": tags}

    @staticmethod
    def _tags_from_page_title(doc_tree, topic: str):
        """Subrubrics dropped from the site menu (e.g. «Дом», «Инвестиции») are still in
        the page title: "<og:title>: <tags>: <topic>: Lenta.ru"."""
        page_title = doc_tree.find("meta", attrs={"property": "title"})
        og_title = doc_tree.find("meta", attrs={"property": "og:title"})
        if not page_title or not og_title:
            return None

        page_title, og_title = page_title.get("content", ""), og_title.get("content", "")
        prefix, suffix = f"{og_title}: ", f": {topic}: Lenta.ru"
        if not (page_title.startswith(prefix) and page_title.endswith(suffix)):
            return None

        tags = page_title[len(prefix):len(page_title) - len(suffix)]
        return tags or None

    @staticmethod
    def _extract_urls_from_html(html: str):
        doc_tree = BeautifulSoup(html, LentaParser.default_parser)
        news_list = doc_tree.find_all("li", "archive-page__item _news")

        next_page_url = None
        load_mores = doc_tree.find_all("a", class_="loadmore js-loadmore _two-buttons")
        for load_more in load_mores:
            if load_more and "Дальше" in load_more.get_text() and "_disabled" not in load_more.get("class", []):
                next_page_url = f"https://lenta.ru{load_more['href']}"
                break

        news_urls = tuple(f"https://lenta.ru{news.find('a')['href']}" for news in news_list)
        return news_urls, next_page_url

    async def _fetch_all_news_on_page(self, initial_html: str):
        news_urls = []
        html = initial_html

        while True:
            page_news_urls, next_page_url = await asyncio.to_thread(self._extract_urls_from_html, html)
            news_urls.extend(page_news_urls)

            if next_page_url:
                html = await self.fetch(next_page_url)
            else:
                break

        # Fetching news (keep each task keyed by its own URL so text and URL stay paired)
        tasks = {
            url: asyncio.create_task(self.fetch(url))
            for url in dict.fromkeys(news_urls)
            if url not in self._seen_urls
        }

        fetched_raw_news = {}

        for url, task in tasks.items():
            try:
                fetch_res = await task
            except aiohttp.ClientResponseError as exc:
                logger.error(f"Cannot fetch {exc.request_info.url}: {exc}")
            except asyncio.TimeoutError:
                logger.error(f"Cannot fetch {url}: Timeout")
            except aiohttp.ClientError as exc:
                logger.error(f"Cannot fetch {url}: {type(exc).__name__}: {exc}")
            else:
                fetched_raw_news[url] = fetch_res

        parse_tasks = {url: asyncio.to_thread(self.parse_article_html, html) for url, html in fetched_raw_news.items()}
        parsed_news = []

        for url, task in parse_tasks.items():
            try:
                parse_res = await task
            except Exception as e:
                logger.exception(f"Cannot parse {url}: {e}")
            else:
                parse_res["url"] = url
                date_match = re.search(r"/news/(\d{4})/(\d{2})/(\d{2})/", url)
                parse_res["date"] = "-".join(date_match.groups()) if date_match else None
                parsed_news.append(parse_res)

        if parsed_news:
            self.writer.writerows(parsed_news)
            self._n_downloaded += len(parsed_news)
            self._seen_urls.update(news["url"] for news in parsed_news)

        return len(parsed_news)

    async def shutdown(self):
        if self._sess is not None:
            await self._sess.close()

        await asyncio.sleep(0.5)

        if self._outfile is not None:
            self._outfile.close()

        self._executor.shutdown(wait=True)

        logger.info(f"{self._n_downloaded} news saved at {self._outfile_name}")

    async def _producer(self):
        for date in self.dates_countdown:
            news_page_url = f"{self._endpoint}/{date}"

            try:
                html = await asyncio.create_task(self.fetch(news_page_url))
            except aiohttp.ClientResponseError:
                logger.exception(f"Cannot fetch {news_page_url}")
            except aiohttp.ClientConnectionError:
                logger.exception(f"Cannot fetch {news_page_url}")
            except BaseException as e:
                logger.info(f"Cannot fetch {news_page_url}: {e}")
            else:
                n_proccessed_news = await self._fetch_all_news_on_page(html)

                if n_proccessed_news == 0:
                    logger.info(f"News not found at {news_page_url}.")

                logger.info(
                    f"{news_page_url} processed ({n_proccessed_news} news). "
                    f"{self._n_downloaded} news saved totally."
                )

    async def run(self):
        try:
            await self._producer()
        finally:
            await self.shutdown()


def main():
    parser = argparse.ArgumentParser(description="Downloads news from Lenta.Ru")

    parser.add_argument(
        "--outfile", default="lenta-ru-news.csv", help="name of result file"
    )

    parser.add_argument(
        "--cpu-workers", default=cpu_count(), type=int, help="number of workers"
    )

    parser.add_argument(
        "--from-date",
        default="30.08.1999",
        type=str,
        help="download news from this date. Example: 30.08.1999",
    )

    parser.add_argument(
        "--to-date",
        default=None,
        type=str,
        help="download news up to this date inclusive (default: today). Example: 31.12.2023",
    )

    args = parser.parse_args()

    parser = LentaParser(
        max_workers=args.cpu_workers,
        outfile_name=args.outfile,
        from_date=args.from_date,
        to_date=args.to_date,
    )

    try:
        asyncio.run(parser.run())
    except KeyboardInterrupt:
        asyncio.run(parser.shutdown())
        logger.info("KeyboardInterrupt, exiting...")


if __name__ == "__main__":
    main()
