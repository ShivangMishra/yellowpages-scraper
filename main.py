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
from dotenv import load_dotenv
import sys
import sqlite3

now = time.strftime('%Y-%m-%d_%H-%M-%S')

def setup_logging(log_filepath=None):
    """Configure logging to output to both file and console"""
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    if log_filepath is None:
        return

    log_dir = os.path.dirname(log_filepath)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    if not os.path.exists(log_filepath):
        with open(log_filepath, 'w') as file:
            file.write('')
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)


def resource_path(relative_path):
    """Get absolute path to resource, works for dev and for PyInstaller"""
    try:
        logging.info(f"Getting resource path for {relative_path}")
        if getattr(sys, 'frozen', False):
            base_path = os.path.dirname(sys.executable)
        else:
            base_path = os.path.dirname(os.path.abspath(__file__))
        logging.info(f"Base path: {base_path}\n")
        return os.path.join(base_path, relative_path)
    except Exception:
        # Fallback to current directory if any error occurs
        logging.error(f"Error getting resource path for {relative_path}")
        return os.path.join(os.getcwd(), relative_path)


def ensure_env_file():
    env_path = resource_path('.env')
    if not os.path.exists(env_path):
        logging.error(f"Environment file not found: {env_path}")
        logging.error("Please create a .env file in the root directory with the required environment variables")
        sys.exit(1)
    return env_path


env_path = ensure_env_file()
load_dotenv(env_path)

BASE_URL = os.getenv('BASE_URL', 'https://www.yellowpages.com.au/find')
SHOULD_LOG_STATES = os.getenv('SHOULD_LOG_STATES', 'False').lower() == 'true'
EMAIL_REGEX = r'[a-z0-9\.-]+@[a-z0-9\.-]+(?=\b|[^a-z0-9._%+-])'
USE_SQLITE = os.getenv('USE_SQLITE', 'True').lower() == 'true'
BASE_PATH = resource_path('')

INPUT_DIRECTORY = os.getenv('INPUT_DIRECTORY', 'inputs')
STATES_FILENAME = os.getenv('STATES_FILENAME', 'states.txt')
CATEGORIES_FILENAME = os.getenv('CATEGORIES_FILENAME', 'categories.txt')
LOG_DIRECTORY = os.path.join(BASE_PATH, 'logs')

OUTPUT_DIRECTORY = os.path.join(BASE_PATH, 'outputs', now)

STATES_FILEPATH = os.path.join(INPUT_DIRECTORY, STATES_FILENAME)
CATEGORIES_FILEPATH = os.path.join(INPUT_DIRECTORY, CATEGORIES_FILENAME)
DB_PATH = os.path.join(BASE_PATH, 'yellowpages.db')
setup_logging(os.path.join(LOG_DIRECTORY, f'log_{now}.log'))


def create_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS yellowpages (
            name TEXT,
            contact TEXT,
            primary_email TEXT,
            state TEXT,
            source_url TEXT,
            owner TEXT,
            urls TEXT, -- Multiple urls separated by ';'
            emails TEXT, -- Multiple emails separated by ';'
            interests TEXT, -- Multiple interests separated by ';'
            category TEXT,
            page INTEGER
        )
    ''')
    conn.commit()
    conn.close()


def get_processed_items_from_db(source_url):
    if not USE_SQLITE:
        return None
    with sqlite3.connect(DB_PATH) as conn:
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT name, contact, primary_email, state, source_url, urls, emails, owner, interests, category, page FROM yellowpages WHERE source_url = ?', (source_url,))
            rows = cursor.fetchall()
        except sqlite3.Error as error:
            logging.error(f"Error fetching processed items from database: {error}")
            return None

    processed_items = []
    for row in rows:
        processed_item = {
            'name': row[0],
            'contact': row[1],
            'email (yellowpages)': row[2],
            'state': row[3],
            'source_url': row[4],
            'owner': row[7],
            'interests': row[8],
            'category': row[9],
            'page': row[10],
        }
        urls = row[5].split(';')
        emails = row[6].split(';')
        for i, url in enumerate(urls):
            processed_item[f'url - {i + 1}'] = url
        for i, email in enumerate(emails):
            processed_item[f'email - {i + 1}'] = email
        processed_items.append(processed_item)
    return processed_items


def save_processed_items_to_db(source_url, processed_items: list[dict]):
    if not USE_SQLITE:
        return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    for item in processed_items:
        urls = ';'.join([item[url_key] for url_key in [key for key in item.keys() if key.startswith('url -')]])
        emails = ';'.join([item[email_key] for email_key in [key for key in item.keys() if key.startswith('email -')]])
        cursor.execute('''
            INSERT INTO yellowpages (name, contact, primary_email, state, source_url, urls, emails, owner, interests, category, page)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
                item['name'],
                item['contact'],
                item['email (yellowpages)'],
                item['state'],
                source_url,
                urls,
                emails, # Assuming emails and interests are not provided in the example
                item['owner'],
                item['interests'],
                item['category'],
                item['page'],
            )
        )
    conn.commit()
    conn.close()


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
    logging.debug(f"Scraping emails from {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        emails = re.findall(EMAIL_REGEX, soup.text)
        for email in emails:
            if email not in scraped_emails:
                scraped_emails.add(email)
    except requests.RequestException as e:
        logging.error(f"Failed to fetch email from {url}")
        logging.debug(f"Error when fetching email from {url}: {e}")
        return None
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


def process_item(item, source_url, page, category):
    name = item['name']
    address_view = item['addressView'] or {'state': "Not Listed"}
    state = address_view['state']
    primary_email = item['primaryEmail']
    contact_number = item['callContactNumber']['value']
    external_links = item['externalLinks'] or []
    processed_item = {
        'name': name,
        'state': state,
        'page': page,
        'category': category,
        'email (yellowpages)': primary_email,
        'contact': contact_number,
        'source_url': source_url,
        'owner': '',
        'interests': '',
    }
    scraped_emails = set()
    root_domains = set()
    bad_domains = set()
    urls = set()
    logging.info(f"Scraping emails for {name} - {contact_number} in {state}")
    for link in external_links:
        if not link['url']:
            continue
        processed_item[f"url - {len(urls) + 1}"] = link['url']

        if is_excluded_domain(link['url']):
            continue

        root_domain = get_root_domain(link['url'])
        if root_domain in bad_domains:
            continue
        if root_domain is not None and root_domain not in root_domains:
            root_domains.add(root_domain)
            emails = scrape_emails_from_url(root_domain)
            if emails is None: # If failed to scrape emails, skip to next link
                bad_domains.add(root_domain)
                continue
            for email in emails:
                if email not in scraped_emails:
                    processed_item[
                        f"email - {len(scraped_emails) + 1}"] = email
                    scraped_emails.add(email)

        emails = scrape_emails_from_url(link['url'])
        if emails is None: # If failed to scrape emails, skip to next link
            continue
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
            logging.debug(f"Error parsing JSON data: {state_json}")
    return None


def scrape_yellowpages_au(base_url, state, category, filename, page=1):
    results = []
    pages_scraped = page - 1
    total_pages = 1
    while pages_scraped < total_pages:
        current_page = pages_scraped + 1
        source_url = base_url + (f"/page-{current_page}" if current_page > 1 else "")
        params = {}

        processed_items_from_db = get_processed_items_from_db(source_url)
        if processed_items_from_db and len(processed_items_from_db) > 0:
            logging.info(f"Found {len(processed_items_from_db)} items in the database for {filename} page {current_page}")
            append_to_excel(f'{OUTPUT_DIRECTORY}/' + filename + '.xlsx', processed_items_from_db, sheet_name=state)
            pages_scraped += 1
            total_pages += 1 # we assume there are more pages to scrape
            continue

        logging.info(f"Sending request for {filename} page {current_page}")
        try:
            response = requests.get(source_url, params=params, headers=HEADERS)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Request failed for {filename} page {current_page}")
            logging.debug(f"Error when fetching {filename} page {current_page}: {e}")
            break

        logging.info(f"Scraping {filename} page {current_page}")

        # Extract window.__INITIAL_STATE__
        initial_state = extract_initial_state(response.text)
        if initial_state:
            logging.info(f"extracted state for page {current_page}")
            if SHOULD_LOG_STATES:
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
            processed_items = [process_item(item, source_url, current_page, category) for item in items]
            append_to_excel(f'{OUTPUT_DIRECTORY}/' + filename + '.xlsx',processed_items, sheet_name=state)
            save_processed_items_to_db(source_url, processed_items)
            pages_scraped += 1
            if current_page >= total_pages:
                logging.info("Reached last page")
                break
        else:
            logging.error(f"Failed to extract window.__INITIAL_STATE__ for {filename} page {current_page}")
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


def check_and_create_db():
    if not os.path.exists(DB_PATH):
        logging.info(f"Database not found. Creating new database at {DB_PATH}")
        create_table()


if __name__ == "__main__":
    if not os.path.exists(OUTPUT_DIRECTORY):
        os.makedirs(OUTPUT_DIRECTORY)

    states = []
    categories = []
    
    with open(STATES_FILEPATH, mode='r') as states_file:
        states = [line.strip() for line in states_file.readlines()]
    with open(CATEGORIES_FILEPATH, mode='r') as categories_file:
        categories = [sanitize_category(line) for line in categories_file.readlines()]

    if USE_SQLITE:
        check_and_create_db()

    for category in categories:
        for state in states:
            base_url = f"{BASE_URL}/{category.lower().replace(' ', '-')}/{state.lower()}"
            scrape_yellowpages_au(base_url, state, category, f'{category}')
