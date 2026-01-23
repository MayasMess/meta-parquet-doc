"""Shared fixtures for tests."""

import pandas as pd
import pyarrow as pa
import pytest


@pytest.fixture
def sample_pandas_df():
    """A small pandas DataFrame for testing."""
    return pd.DataFrame({
        "user_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["a@x.com", "b@x.com", None],
    })


@pytest.fixture
def sample_pyarrow_table():
    """A small PyArrow Table for testing."""
    return pa.table({
        "user_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["a@x.com", "b@x.com", None],
    })


@pytest.fixture
def sample_columns_metadata():
    """Valid column metadata dict."""
    return {
        "user_id": {"description": "User ID", "nullable": False, "pii": False},
        "name": {"description": "Full name", "nullable": False, "pii": True},
        "email": {"description": "Email address", "nullable": True, "pii": True, "pii_category": "contact"},
    }


@pytest.fixture
def sample_dataset_metadata():
    """Valid dataset metadata dict."""
    return {"description": "Test user dataset", "owner": "test-team"}
