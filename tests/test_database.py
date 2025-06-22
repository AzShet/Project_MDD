import pytest
from unittest.mock import patch

# It's better to import the specific module or function you are testing
# The conftest.py should handle making 'src' importable
try:
    from database.mongo_connection import get_db
except ImportError:
    # This allows an alternative import path if running tests in a way that src. is required
    from src.database.mongo_connection import get_db


def test_get_db_exists_and_callable():
    """Test that the get_db function exists and can be called."""
    assert callable(get_db), "get_db should be a callable function"

@patch('database.mongo_connection.MongoClient') # Mocks MongoClient within mongo_connection
def test_get_db_call_mongodb_mocked(mock_mongo_client):
    """
    Test calling get_db with MongoClient mocked.
    This prevents actual DB connection attempts during tests.
    """
    # Configure the mock MongoClient instance
    mock_instance = mock_mongo_client.return_value
    mock_instance.admin.command.return_value = {"ok": 1} # Mock a successful ping

    db = get_db()

    # Check that MongoClient was called (i.e., an attempt to connect was made)
    mock_mongo_client.assert_called_once()

    # Check that the ping command was attempted
    mock_instance.admin.command.assert_called_once_with("ping")

    # Check that a database object (or None if ping failed, though we mocked success) is returned
    # In this mocked scenario, it should return a database object from the client.
    assert db is not None, "get_db should return a database object on successful (mocked) connection"
    assert db == mock_instance["data_casinos"], "Should return the 'data_casinos' DB from the mocked client"

@patch('database.mongo_connection.MongoClient')
def test_get_db_connection_error_mocked(mock_mongo_client):
    """
    Test calling get_db when MongoClient fails to connect (mocked error).
    """
    # Configure the mock MongoClient to raise an exception
    mock_mongo_client.side_effect = Exception("Mock connection error")

    db = get_db()

    mock_mongo_client.assert_called_once() # MongoClient was still called
    assert db is None, "get_db should return None when a connection error occurs"
