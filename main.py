import csv
import json
import logging
import math
import os
import random
import re
import time
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = 'https://www.yellowpages.com.au/find'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

EMAIL_REGEX = r'[a-z0-9\.-]+@[a-z0-9\.-]+(?=\b|[^a-z0-9._%+-])'

# List of user agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
    # 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    # 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    # 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
    # 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.59 Safari/537.36',
    # 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.38 Safari/537.36'
]

HEADERS = {'User-Agent': random.choice(USER_AGENTS)}

# List of domains to exclude from scraping
# These domains are usually social media sites, search engines, etc.
# Add more domains to exclude as needed
EXCLUDED_DOMAINS = [
    'facebook.com',
    'twitter.com',
    'instagram.com',
    'linkedin.com',
    'youtube.com',
    'pinterest.com',
    'wikipedia.org',
    'yelp.com',
    'tripadvisor.com',
    'yellowpages.com',
    'yellowpages.com.au',
    'truelocal.com.au',
    'whitepages.com.au',
    'truelocal.com.au',
    'localsearch.com.au',
    'startlocal.com.au',
    'aussieweb.com.au',
    'hipages',
    'whereis.com',
]

OUTPUT_DIRECTORY = 'outputs'
LOG_DIRECTORY = 'state-logs'


def is_excluded_domain(url):
    return any(domain in url for domain in EXCLUDED_DOMAINS)


def get_root_domain(url):
    try:
        parsed_url = urlparse(url)
        root_domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
        return root_domain
    except Exception as e:
        logging.error(f"Error parsing URL {url}: {e}")
        return None


def scrape_emails_from_url(url):
    scraped_emails = set()
    logging.info(f"Scraping emails from {url}")
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        emails = re.findall(EMAIL_REGEX, soup.text)
        for email in emails:
            if email not in scraped_emails:
                scraped_emails.add(email)
    except requests.RequestException as e:
        logging.error(f"Failed to fetch email from {url}: {e}")
    return scraped_emails


def append_to_excel(file_path, new_data):
    try:
        existing_df = pd.read_excel(file_path)
    except FileNotFoundError:
        existing_df = pd.DataFrame()

    new_df = pd.DataFrame(new_data)
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df.to_excel(file_path, index=False)


def process_item(item):
    name = item['name']
    addressView = item['addressView'] or {'state': "Not Listed"}
    state = addressView['state']
    primaryEmail = item['primaryEmail']
    contactNumber = item['callContactNumber']['value']
    externalLinks = item['externalLinks'] or []
    processed_item = {
        'name': name,
        'state': state,
        'email (yellowpages)': primaryEmail,
        'contact number': contactNumber
    }
    scraped_emails = set()
    root_domains = set()
    urls = set()
    for link in externalLinks:
        if not link['url']:
            continue
        processed_item[f"url - {len(urls) + 1}"] = link['url']

        if is_excluded_domain(link['url']):
            continue

        root_domain = get_root_domain(link['url'])
        if root_domain and root_domain not in root_domains:
            # uncomment if you want the base urls separately
            # processed_item[f"base url - {len(root_domains) + 1}"] = root_domain
            root_domains.add(root_domain)
            emails = scrape_emails_from_url(root_domain)
            for email in emails:
                if email not in scraped_emails:
                    processed_item[
                        f"email - {len(scraped_emails) + 1}"] = email
                    scraped_emails.add(email)

        emails = scrape_emails_from_url(link['url'])
        for email in emails:
            if email not in scraped_emails:
                processed_item[f"email - {len(scraped_emails) + 1}"] = email
                scraped_emails.add(email)

    return processed_item


def extract_initial_state(html_content):
    pattern = r'window\.__INITIAL_STATE__\s*=\s*({.*?});'
    match = re.search(pattern, html_content, re.DOTALL)
    if match:
        state_json = match.group(1)
        try:
            return json.loads(state_json)
        except json.JSONDecodeError:
            logging.error("Failed to parse JSON data")
    return None


def scrape_yellowpages_au(base_url, filename, page=1):

    results = []
    pagesScraped = page - 1
    totalPages = 1
    while pagesScraped < totalPages:
        currentPage = pagesScraped + 1
        logging.info(f"Sending request for {filename} page {currentPage}")
        url = base_url + (f"/page-{currentPage}" if currentPage > 1 else "")
        params = {
            # "clue": search_term,
            # "locationClue": location,
            # "pageNumber": page
        }

        try:
            response = requests.get(url, params=params, headers=HEADERS)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Request failed: {e}")
            break

        logging.info(f"Scraping {filename} page {currentPage}")

        # Extract window.__INITIAL_STATE__
        initial_state = extract_initial_state(response.text)
        if initial_state:
            logging.info(f"extracted state for page {currentPage}")
            with open(f'{LOG_DIRECTORY}/{filename}-{currentPage}.json',
                      'w') as file:
                json.dump(initial_state, file, indent=4)

            pagination = initial_state['model']['pagination']
            searchResultsPerPage = pagination['searchResultsPerPage']
            totalResults = pagination['totalResults']
            totalPages = math.ceil(totalResults / searchResultsPerPage)

            if pagination['currentPage'] != currentPage:
                logging.error("PAGINATION CURRENT PAGE MISMATCH!")
                logging.error(
                    f"currentPage = {currentPage}, pagination.currentPage = {pagination['currentPage']}"
                )
                break

            items = initial_state['model']['inAreaResultViews']
            processedItems = [process_item(item) for item in items]
            append_to_excel(f'{OUTPUT_DIRECTORY}/' + filename + '.xlsx',
                            processedItems)

            pagesScraped += 1
            if currentPage >= totalPages:
                logging.info("Reached last page")
                break
        else:
            logging.info("failed to extract state")
            logging.warning("Failed to extract window.__INITIAL_STATE__")
            break

        time.sleep(random.uniform(1, 3))

    return results


if __name__ == "__main__":
    csv_file_path = 'inputs.csv'
    with open(csv_file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            category = row['category']
            state = row['state']
            try:
                os.mkdir(f"{OUTPUT_DIRECTORY}/{state}")
                os.mkdir(f"{LOG_DIRECTORY}/{state}")
                logging.info(f"Created directory: '{state}'")
            except Exception as e:
                print(
                    f"An error occurred while creating directory {state}: {e}")
            base_url = f"{BASE_URL}/{category.lower().replace(' ', '-')}/{state.lower()}"
            scrape_yellowpages_au(base_url, f'{state}/{category}')
