import sys
import os

# Add the src directory to the Python path
# This allows pytest to find modules in src/ (e.g., src.database.mongo_connection)
# when tests are run from the root directory.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
