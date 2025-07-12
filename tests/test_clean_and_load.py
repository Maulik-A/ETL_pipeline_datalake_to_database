import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src import clean_and_load

def test_cleaning_data():
    df = pd.DataFrame({
        'a': [1, 1, 2, None],
        'Production Code': ['AB001', 'CD001', 'EF002', 'AB003']
    })
    result = clean_and_load.cleaning_data(df)
    assert 'processing_ts' in result.columns
    assert result['a'].isnull().sum() == 0
    assert result.duplicated().sum() == 0

def test_load_to_postgres_success():
    df = pd.DataFrame({'a': [1], 'Production Code': ['AB001']})
    with patch('src.clean_and_load.create_engine') as mock_engine:
        mock_conn = MagicMock()
        mock_engine.return_value = mock_conn
        with patch.object(df, 'to_sql') as mock_to_sql:
            clean_and_load.load_to_postgres(df, 'table')
            mock_to_sql.assert_called_once()

def test_load_to_postgres_failure():
    df = pd.DataFrame({'a': [1], 'Production Code': ['AB001']})
    with patch('src.clean_and_load.create_engine', side_effect=Exception('fail')):
        with pytest.raises(Exception):
            clean_and_load.load_to_postgres(df, 'table')

def test_process_file_success(monkeypatch):
    # Mock S3 and extract functions
    s3_client = MagicMock()
    s3_client.list_objects_v2.return_value = {'Contents': [{'Key': 'file.csv'}]}
    monkeypatch.setattr(clean_and_load, 'list_s3_files', lambda *a, **kw: ['file.csv'])
    monkeypatch.setattr(clean_and_load, 'read_csv_file', lambda *a, **kw: pd.DataFrame({'Production Code': ['AB001'], 'a': [1]}))
    monkeypatch.setattr(clean_and_load, 'validate_csv_schema_by_production_code', lambda df, schemas: (True, 'ok'))
    monkeypatch.setattr(clean_and_load, 'drop_invalid_production_codes', lambda df: df)
    monkeypatch.setattr(clean_and_load, 'cleaning_data', lambda df: df)
    monkeypatch.setattr(clean_and_load, 'load_to_postgres', lambda df, table: None)
    clean_and_load.process_file(s3_client, 'bucket', 'prefix')

def test_process_file_schema_fail(monkeypatch):
    s3_client = MagicMock()
    monkeypatch.setattr(clean_and_load, 'list_s3_files', lambda *a, **kw: ['file.csv'])
    monkeypatch.setattr(clean_and_load, 'read_csv_file', lambda *a, **kw: pd.DataFrame({'Production Code': ['AB001'], 'a': [1]}))
    monkeypatch.setattr(clean_and_load, 'validate_csv_schema_by_production_code', lambda df, schemas: (False, 'bad schema'))
    clean_and_load.process_file(s3_client, 'bucket', 'prefix')

def test_process_file_drop_invalid(monkeypatch):
    s3_client = MagicMock()
    monkeypatch.setattr(clean_and_load, 'list_s3_files', lambda *a, **kw: ['file.csv'])
    monkeypatch.setattr(clean_and_load, 'read_csv_file', lambda *a, **kw: pd.DataFrame({'Production Code': ['AB001'], 'a': [1]}))
    monkeypatch.setattr(clean_and_load, 'validate_csv_schema_by_production_code', lambda df, schemas: (True, 'ok'))
    monkeypatch.setattr(clean_and_load, 'drop_invalid_production_codes', lambda df: pd.DataFrame())
    clean_and_load.process_file(s3_client, 'bucket', 'prefix') 