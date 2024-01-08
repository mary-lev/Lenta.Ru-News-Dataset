import argparse
import asyncio
import csv
import logging
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


class LentaParser:

    # lxml is much faster but error prone
    default_parser = "html.parser"

    def __init__(self, *, max_workers: int, outfile_name: str, from_date: str):
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

    @property
    def dates_countdown(self):
        date_start, date_end = self._from_date, datetime.today()

        while date_start <= date_end:
            yield date_start.strftime("%Y/%m/%d")
            date_start += timedelta(days=1)

    @property
    def writer(self):
        if self._csv_writer is None:
            self._outfile = open(self._outfile_name, "w", 1)
            self._csv_writer = csv.DictWriter(
                self._outfile, fieldnames=["url", "title", "text", "topic", "tags"]
            )
            self._csv_writer.writeheader()

        return self._csv_writer

    @property
    def session(self):
        if self._sess is None or self._sess.closed:

            self._connector = aiohttp.TCPConnector(
                use_dns_cache=True, ttl_dns_cache=60 * 60, limit=1024
            )
            self._sess = aiohttp.ClientSession(
                connector=self._connector, timeout=self.timeouts
            )

        return self._sess

    async def fetch(self, url: str):
        response = await self.session.get(url, allow_redirects=False)
        response.raise_for_status()
        return await response.text(encoding="utf-8")

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

        return {"title": title, "text": text, "topic": topic, "tags": tags}

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

        # Fetching news
        tasks = [asyncio.create_task(self.fetch(url)) for url in set(news_urls)]

        fetched_raw_news = {}

        for i, task in enumerate(tasks):
            try:
                fetch_res = await task
            except aiohttp.ClientResponseError as exc:
                logger.error(f"Cannot fetch {exc.request_info.url}: {exc}")
            except asyncio.TimeoutError:
                logger.exception("Cannot fetch. Timeout")
            else:
                fetched_raw_news[news_urls[i]] = fetch_res

        parse_tasks = {url: asyncio.to_thread(self.parse_article_html, html) for url, html in fetched_raw_news.items()}
        parsed_news = []

        for url, task in parse_tasks.items():
            try:
                parse_res = await task
            except Exception as e:
                logger.exception(f"Cannot parse {url}: {e}")
            else:
                parse_res["url"] = url
                parsed_news.append(parse_res)

        if parsed_news:
            self.writer.writerows(parsed_news)
            self._n_downloaded += len(parsed_news)

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

    args = parser.parse_args()

    parser = LentaParser(
        max_workers=args.cpu_workers,
        outfile_name=args.outfile,
        from_date=args.from_date,
    )

    try:
        asyncio.run(parser.run())
    except KeyboardInterrupt:
        asyncio.run(parser.shutdown())
        logger.info("KeyboardInterrupt, exiting...")


if __name__ == "__main__":
    main()
