# Eurostat Downloader

Crawls the Statistics Finland website for a trilingual semi-aligned corpus of statistical texts (as HTML) and associated tables (also as HTML).

## Requirements
 * Python 3
 * requests
 * BeautifulSoup 4

## Usage
```
usage: statfi_crawler.py [-h] [-s STAGGER] [-q] output_dir

Crawl a corpus out of the Statistics Finland website.

positional arguments:
  output_dir            Local file system target to store the data in.

optional arguments:
  -h, --help            show this help message and exit
  -s STAGGER, --stagger STAGGER
                        The minimum amount of time (in milliseconds) that
                        needs to pass between two subsequent HTTP requests.
  -q, --quiet           Only output errors to the STDERR stream.
```
