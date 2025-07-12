import pytest
import pandas as pd
from unittest.mock import MagicMock
from src import extract

class DummyS3Client:
    def __init__(self, files=None):
        self.files = files or []
    def list_objects_v2(self, Bucket, Prefix):
        return {'Contents': [{'Key': f} for f in self.files]}
    def get_object(self, Bucket, Key):
        if Key == 'valid.csv':
            from io import StringIO
            return {'Body': StringIO('Production Code,Unit ID\nAB001,123')}
        raise Exception('File not found')

def test_list_s3_files():
    client = DummyS3Client(files=['a.csv', 'b.txt', 'c.csv'])
    result = extract.list_s3_files(client, 'bucket', 'prefix')
    assert result == ['a.csv', 'c.csv']

def test_is_valid_csv_filename():
    assert extract.is_valid_csv_filename('dir/AB001.csv', 'dir', 'AB')
    assert not extract.is_valid_csv_filename('dir/AB01.csv', 'dir', 'AB')
    assert not extract.is_valid_csv_filename('other/AB001.csv', 'dir', 'AB')

def test_read_csv_file_success():
    client = DummyS3Client(files=['valid.csv'])
    df = extract.read_csv_file(client, 'bucket', 'valid.csv')
    assert isinstance(df, pd.DataFrame)
    assert 'Production Code' in df.columns

def test_read_csv_file_failure():
    client = DummyS3Client(files=['invalid.csv'])
    with pytest.raises(Exception):
        extract.read_csv_file(client, 'bucket', 'notfound.csv')

def test_validate_csv_schema_by_production_code_match():
    df = pd.DataFrame({
        'Production Code': ['AB001'],
        'Parent ID': ['P1'],
        'Child Position': ['C1'],
        'Operator': ['O1'],
        'Column A Stage A': [1],
        'Column B Stage A': [2],
    })
    schemas = {'AB': list(df.columns)}
    valid, msg = extract.validate_csv_schema_by_production_code(df, schemas)
    assert valid
    assert 'match' in msg

def test_validate_csv_schema_by_production_code_header_rename():
    df = pd.DataFrame({
        'Production Code': ['AB001'],
        'Parent ID': ['P1'],
        'C': ['C1'],
        'D': ['O1'],
        'E': [1],
        'F': [2],
    })
    schemas = {'AB': ['Production Code', 'Parent ID', 'Child Position', 'Operator', 'Column A Stage A', 'Column B Stage A']}
    valid, res = extract.validate_csv_schema_by_production_code(df, schemas)
    assert valid
    assert isinstance(res, pd.DataFrame)
    assert list(res.columns) == schemas['AB']

def test_validate_csv_schema_by_production_code_missing_column():
    df = pd.DataFrame({'A': [1]})
    schemas = {'AB': ['A']}
    valid, msg = extract.validate_csv_schema_by_production_code(df, schemas)
    assert not valid
    assert 'Missing required column' in msg

def test_drop_invalid_production_codes():
    df = pd.DataFrame({'Production Code': ['AB001', 'ZZ999', 'BAD', 'A1234']})
    filtered = extract.drop_invalid_production_codes(df)
    assert all(filtered['Production Code'].str.match(r'^[A-Z]{2}\d{3}$'))

def test_drop_invalid_production_codes_missing_column():
    df = pd.DataFrame({'A': [1]})
    with pytest.raises(ValueError):
        extract.drop_invalid_production_codes(df) 