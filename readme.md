# YellowPages Scraper

This project is a web scraper for extracting business information from the YellowPages Australia website. It collects data such as business names, contact details, emails, and more, and stores the information in an SQLite database and Excel files.

## Features

- Scrapes business information from YellowPages Australia
- Stores data in an SQLite database
- Saves data to Excel files
- Configurable via environment variables
- Supports logging to both console and file

## Requirements

- Python 3.10 or higher
- Poetry for dependency management

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/yellowpages-scraper.git
    cd yellowpages-scraper
    ```

2. Install dependencies using Poetry:
    ```sh
    poetry install
    ```

3. Create a `.env` file in the root directory based on the `.env.example` file:
    ```sh
    cp .env.example .env
    ```

4. Update the `.env` file with the required environment variables.

## Usage

1. Ensure you have the input files `states.txt` and `categories.txt` in the `inputs` directory. These files should contain the states and categories to scrape, respectively.

2. Run the scraper:
    ```sh
    poetry run python main.py
    ```

## Configuration

The scraper can be configured using environment variables defined in the `.env` file:

- `INPUT_DIRECTORY`: Directory containing input files (default: `inputs`)
- `STATES_FILENAME`: Filename for the states list (default: `states.txt`)
- `CATEGORIES_FILENAME`: Filename for the categories list (default: `categories.txt`)
- `BASE_URL`: Base URL for YellowPages Australia (default: `https://www.yellowpages.com.au/find`)
- `SHOULD_LOG_STATES`: Whether to log the extracted state data (default: `false`)
- `USE_SQLITE`: Whether to use SQLite for storing data (default: `true`)

## Logging

Logs are saved to the `logs` directory with a timestamped filename. Both console and file logging are supported.
