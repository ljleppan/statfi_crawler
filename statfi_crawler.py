import argparse
from pathlib import Path
import re
import time
from typing import Any, List

from bs4 import BeautifulSoup, Tag
import requests

DEFAULT_STAGGER_MS = 1000


class StatfiCrawler(object):
    def __init__(
        self, stagger_ms: int = DEFAULT_STAGGER_MS, quiet: bool = False
    ) -> None:
        self.stagger_ms = stagger_ms
        self.last_get_sent = 0
        self.quiet = quiet

    def _print(self, *args: Any) -> None:
        if not self.quiet:
            print(*args)

    def _current_time_ms(self):
        return int(round(time.time() * 1000))

    def _get_html(self, url: str) -> BeautifulSoup:
        url = "http://www.tilastokeskus.fi" + url
        remaining_stagger = (
            self.last_get_sent + self.stagger_ms - self._current_time_ms()
        )
        if remaining_stagger > 0:
            time.sleep(remaining_stagger / 1000)
        response = requests.get(url)
        self.last_get_sent = self._current_time_ms()
        return BeautifulSoup(response.text, "html.parser")

    def _crawl_urls_to_datasets(self) -> List[str]:
        html = self._get_html("/til/aiheet.html")
        anchors = html.find_all(href=re.compile("/til/\w+/index.html"))
        return [a["href"].split("/")[-2] for a in anchors]

    def _crawl_urls_to_dataset_publications(self, stat_name: str) -> List[str]:
        html = self._get_html("/til/{}/tie.html".format(stat_name))
        anchors = html.find_all(
            href=re.compile("/til/{}/.*_tie_\d+_fi.html".format(stat_name))
        )
        return [a["href"] for a in anchors]

    def _crawl_primary_content(self, url: str) -> Tag:
        html = self._get_html(url)
        return html.find(id="content")

    def _get_appendix_table_urls(self, report_url: str, html: Tag) -> List[str]:
        release_prefix, language = re.match(
            r"(.*)_tie_\d+_(en|fi|sv)\.html", report_url
        ).groups()
        appendix_table_regex = re.compile(
            r".*{}_tau_\d+_{}.html".format(release_prefix, language)
        )
        anchors = html.find_all(href=appendix_table_regex)
        return [a["href"] for a in anchors]

    def _normalized_filename(self, filename: str) -> str:
        filename = filename.replace("/", "_")
        if filename[0] == "_":
            filename = filename[1:]
        return filename

    def _fetch_publication(
        self, language_specific_publication_url: str, output_dir: Path
    ) -> BeautifulSoup:
        filename = self._normalized_filename(language_specific_publication_url)
        publication_file = output_dir / filename
        if publication_file.exists():
            self._print("\tSKIP {}, already on disk".format(publication_file))
            content = BeautifulSoup(publication_file.read_text(), "lxml")
        else:
            content = self._crawl_primary_content(language_specific_publication_url)
            publication_file.write_text(str(content))
            self._print("\tSaved to", publication_file)
        return content

    def _store_appendix_table(self, appendix_table_url: str, output_dir: Path) -> None:
        appendix_table_file = output_dir / self._normalized_filename(appendix_table_url)
        if appendix_table_file.exists():
            self._print("\t\tSKIP, already on disk as {}".format(appendix_table_file))
        else:
            appendix_content = self._crawl_primary_content(appendix_table_url)
            appendix_table_file.write_text(str(appendix_content))
            self._print("\t\tSaved to", appendix_table_file)

    def download_corpus(self, output_dir: Path) -> None:
        datasets = self._crawl_urls_to_datasets()
        for dataset in datasets:
            for publication_url in self._crawl_urls_to_dataset_publications(dataset):
                for language in ["fi", "en", "sv"]:
                    language_specific_publication_url = publication_url.replace(
                        "_fi.html", "_{}.html".format(language)
                    )

                    self._print(language_specific_publication_url)
                    content = self._fetch_publication(
                        language_specific_publication_url, output_dir
                    )

                    self._print("\tChecking appendices")
                    appendix_urls = self._get_appendix_table_urls(
                        language_specific_publication_url, content
                    )
                    for appendix_table_url in appendix_urls:
                        self._store_appendix_table(appendix_table_url, output_dir)


if __name__ == "__main__":

    def dir_path(path: str) -> Path:
        path = Path(path)
        if path.is_dir():
            return path
        raise NotADirectoryError(path)

    parser = argparse.ArgumentParser(
        description="Crawl a corpus out of the Statistics Finland website."
    )
    parser.add_argument(
        "-s",
        "--stagger",
        type=int,
        default=DEFAULT_STAGGER_MS,
        help="The minimum amount of time (in milliseconds) that needs to pass between two subsequent HTTP requests.",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Only output errors to the STDERR stream.",
    )
    parser.add_argument(
        "output_dir",
        type=dir_path,
        help="Local file system target to store the data in.",
    )
    args = parser.parse_args()

    StatFiCrawler(args.stagger, args.quiet).download_corpus(args.output_dir)
