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

INPUT_DIRECTORY = 'inputs'
OUTPUT_DIRECTORY = os.path.join('outputs', time.strftime('%Y-%m-%d_%H-%M-%S'))
LOG_DIRECTORY = 'state-logs'

STATES_FILEPATH = os.path.join(INPUT_DIRECTORY, 'states.txt')
CATEGORIES_FILEPATH = os.path.join(INPUT_DIRECTORY, 'categories.txt')

BASE_URL = 'https://www.yellowpages.com.au/find'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

EMAIL_REGEX = r'[a-z0-9\.-]+@[a-z0-9\.-]+(?=\b|[^a-z0-9._%+-])'

# List of user agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
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
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        emails = re.findall(EMAIL_REGEX, soup.text)
        for email in emails:
            if email not in scraped_emails:
                scraped_emails.add(email)
    except requests.RequestException as e:
        logging.error(f"Failed to fetch email from {url}: {e}")
    return scraped_emails


def append_to_excel(file_path, new_data, sheet_name):
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            try:
                existing_df = pd.read_excel(file_path, sheet_name=sheet_name)
                new_df = pd.DataFrame(new_data)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df.to_excel(writer, sheet_name=sheet_name, index=False)
            except ValueError:
                # If the sheet does not exist, create it
                new_df = pd.DataFrame(new_data)
                new_df.to_excel(writer, sheet_name=sheet_name, index=False)
    except FileNotFoundError:
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            new_df = pd.DataFrame(new_data)
            new_df.to_excel(writer, sheet_name=sheet_name, index=False)

def process_item(item):
    name = item['name']
    address_view = item['addressView'] or {'state': "Not Listed"}
    state = address_view['state']
    primary_email = item['primaryEmail']
    contact_number = item['callContactNumber']['value']
    external_links = item['externalLinks'] or []
    processed_item = {
        'name': name,
        'state': state,
        'email (yellowpages)': primary_email,
        'contact number': contact_number
    }
    scraped_emails = set()
    root_domains = set()
    urls = set()
    for link in external_links:
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
    pages_scraped = page - 1
    total_pages = 1
    while pages_scraped < total_pages:
        current_page = pages_scraped + 1
        logging.info(f"Sending request for {filename} page {current_page}")
        url = base_url + (f"/page-{current_page}" if current_page > 1 else "")
        params = {}

        try:
            response = requests.get(url, params=params, headers=HEADERS)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Request failed: {e}")
            break

        logging.info(f"Scraping {filename} page {current_page}")

        # Extract window.__INITIAL_STATE__
        initial_state = extract_initial_state(response.text)
        if initial_state:
            logging.info(f"extracted state for page {current_page}")
            with open(f'{LOG_DIRECTORY}/{filename}-{current_page}.json',
                      'w') as file:
                json.dump(initial_state, file, indent=4)

            pagination = initial_state['model']['pagination']
            searchResultsPerPage = pagination['searchResultsPerPage']
            totalResults = pagination['totalResults']
            total_pages = math.ceil(totalResults / searchResultsPerPage)

            if pagination['currentPage'] != current_page:
                logging.error("PAGINATION CURRENT PAGE MISMATCH!")
                logging.error(
                    f"currentPage = {current_page}, pagination.currentPage = {pagination['currentPage']}"
                )
                break

            items = initial_state['model']['inAreaResultViews']
            processed_items = [process_item(item) for item in items]
            append_to_excel(f'{OUTPUT_DIRECTORY}/' + filename + '.xlsx',processed_items, sheet_name=state)

            pages_scraped += 1
            if current_page >= total_pages:
                logging.info("Reached last page")
                break
        else:
            logging.info("failed to extract state")
            logging.warning("Failed to extract window.__INITIAL_STATE__")
            break

        time.sleep(random.uniform(1, 3))

    return results

def sanitize_category(category_name):
    # Replace special characters and spaces with hyphens
    category = re.sub(r'[^a-zA-Z0-9]+', '-', category_name)
    # Replace multiple consecutive hyphens with a single hyphen
    category = re.sub(r'-+', '-', category)
    # Remove leading and trailing hyphens
    category = category.strip('-')
    return category

if __name__ == "__main__":
    if not os.path.exists(OUTPUT_DIRECTORY):
        os.makedirs(OUTPUT_DIRECTORY)

    if not os.path.exists(LOG_DIRECTORY):
        os.makedirs(LOG_DIRECTORY)

    states = []
    categories = []
    
    with open(STATES_FILEPATH, mode='r') as states_file:
        states = [line.strip() for line in states_file.readlines()]
    with open(CATEGORIES_FILEPATH, mode='r') as categories_file:
        categories = [sanitize_category(line) for line in categories_file.readlines()]
    
    for category in categories:
        for state in states:
            base_url = f"{BASE_URL}/{category.lower().replace(' ', '-')}/{state.lower()}"
            scrape_yellowpages_au(base_url, f'{category}')
