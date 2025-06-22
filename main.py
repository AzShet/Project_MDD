import sys
import os

# Add src to Python path to allow direct imports of modules under src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

# Try to import the web_scraper module.
# Note: As web_scraper.py currently runs its full logic on import,
# this line alone will trigger the scraping process if the import is successful.
try:
    from mincetur_scraper import web_scraper
    print("Successfully imported web_scraper module.")
except ImportError as e:
    print(f"Error importing web_scraper: {e}")
    print("Please ensure the project structure is correct and all dependencies are installed.")
    web_scraper = None

def main_entry_point():
    print("Executing main script entry point...")
    if web_scraper:
        # If web_scraper.py is refactored into functions in the future,
        # a specific function would be called here.
        # For now, its code has already run if the import was successful.
        print("web_scraper module was imported. Its top-level code has executed.")
    else:
        print("web_scraper module could not be imported. Cannot run scraping logic.")
    print("Main script entry point finished.")

if __name__ == "__main__":
    main_entry_point()
