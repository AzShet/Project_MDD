# Project_MDD

This project is for a data mining course. It includes tools for scraping data from the Mincetur website, processing it, and potentially loading it into a database.

## Project Structure

The project follows a standard Python project layout:

```
.
├── data/                  # For storing data files (e.g., CSVs, Excel outputs)
├── main.py                # Main entry point to run the application
├── requirements.txt       # Python package dependencies
├── src/                   # Source code
│   ├── __init__.py
│   ├── database/          # Database related modules
│   │   ├── __init__.py
│   │   └── mongo_connection.py # MongoDB connection logic
│   └── mincetur_scraper/  # Core package for Mincetur scraping and processing
│       ├── __init__.py
│       ├── web_scraper.py       # Selenium-based web scraper
│       ├── data_loader.py       # For loading data (e.g., into DB)
│       ├── data_transformer.py  # For transforming scraped data
│       ├── nlp_processor.py     # For NLP related tasks
│       └── pdf_extractor.py     # For extracting data from PDFs
├── tests/                 # Test files
│   ├── __init__.py        # (Optional, but good practice for test discovery)
│   ├── conftest.py        # Pytest configuration, e.g., path adjustments
│   ├── test_database.py   # Tests for database connection
│   └── test_placeholder.py# Basic placeholder tests
└── ...                    # Other files like LICENSE, .gitignore etc.
```

*(Note: Due to issues with unicode characters in filenames, some original files under a `main/` directory might still be present in the repository but are superseded by their English-named counterparts in `src/mincetur_scraper/`.)*

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-name>
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Running the Application

To run the main application (which currently executes the web scraper):
```bash
python main.py
```

## Running Tests

To run the automated tests:
```bash
pytest
```
Make sure you have installed `pytest` (it's included in `requirements.txt`). Tests are located in the `tests/` directory.

## Modules

*   **`src/mincetur_scraper/web_scraper.py`**: Contains the logic for scraping data from the Mincetur website using Selenium.
*   **`src/database/mongo_connection.py`**: Handles the connection to the MongoDB database.
*   **Other modules** (`data_loader.py`, `data_transformer.py`, `nlp_processor.py`, `pdf_extractor.py`) are placeholders for future development.

```python
# Create an __init__.py in tests for good measure if it wasn't explicitly planned.
# (It's good for test discovery and making 'tests' a package)
# However, pytest usually discovers tests fine without it too.
# For now, I will skip creating tests/__init__.py unless an issue arises.
```
