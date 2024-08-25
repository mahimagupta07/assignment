import os
import pytest
import pytest_check as check
import yaml
import pandas as pd
from pymongo import MongoClient, errors

@pytest.fixture(scope="module")
def config():
    with open('config.yaml', 'r') as file:
        return yaml.safe_load(file)

@pytest.fixture(scope="module")
def input_location(config):
    return os.path.dirname(config['file']['input'])

@pytest.fixture(scope="module")
def input_file(config):
    return os.path.basename(config['file']['input'])

@pytest.fixture(scope="module")
def output_location(config):
    return os.path.dirname(config['file']['output'])  

@pytest.fixture(scope="module")
def mongo_url(config):
    return config['mongo']['url']

@pytest.fixture(scope="module")
def expected_columns():
    return ['FirstName', 'LastName', 'Company', 'BirthDate', 'Salary', 'Address', 
            'Suburb', 'State', 'Post', 'Phone', 'Mobile', 'Email']

def test_config_file_integrity(config):
    required_keys = ['file', 'mongo', 'database']
    file_keys = ['input', 'output']
    mongo_keys = ['url', 'username', 'password']
    database_keys = ['name', 'collection']
    
    valid_config = all(key in config for key in required_keys) and all(key in config['file'] for key in file_keys) and all(key in config['mongo'] for key in mongo_keys) and all(key in config['database'] for key in database_keys)
                   
    check.equal(valid_config, True)
    print("[✔] Config file is valid" if valid_config else "[✘] Config file is missing required fields")


def test_input_location_exists(input_location):
    exists = os.path.exists(input_location)
    check.equal(exists, True)
    print("[✔] Input location exists" if exists else "[✘] Input location does not exist")

def test_input_location_is_readable(input_location):
    readable = os.access(input_location, os.R_OK)
    check.equal(readable, True)
    print("[✔] Input location is readable" if readable else "[✘] Input location is not readable")

def test_file_found_in_input_location(input_location, input_file):
    file_path = os.path.join(input_location, input_file)
    file_exists = os.path.isfile(file_path)
    check.equal(file_exists, True)
    print(f"[✔] File '{input_file}' found in input location" if file_exists else f"[✘] File '{input_file}' not found in input location")

def test_input_file_is_not_empty(input_location, input_file):
    file_path = os.path.join(input_location, input_file)
    df = pd.read_csv(file_path, sep='|', header=None)
    non_empty = len(df) > 0
    check.equal(non_empty, True)
    print("[✔] Input file is not empty" if non_empty else "[✘] Input file is empty")

def test_date_format_in_input_file(input_location, input_file):
    file_path = os.path.join(input_location, input_file)
    df = pd.read_csv(file_path, sep='|', header=None)
    # Assuming the BirthDate is the 4th column (index 3)
    valid_format = df[3].apply(lambda x: len(str(x)) == 8 and str(x).isdigit())
    all_valid = valid_format.all()
    check.equal(all_valid, True)
    print("[✔] All dates are in valid format" if all_valid else "[✘] Some dates are not in valid format")

def test_no_duplicate_records(input_location, input_file):
    file_path = os.path.join(input_location, input_file)
    df = pd.read_csv(file_path, sep='|', header=None)
    # Assuming duplicates are checked on FirstName, LastName, and Email (indexes 0, 1, and 11)
    duplicate_free = df.duplicated(subset=[0, 1, 11]).sum() == 0
    check.equal(duplicate_free, True)
    print("[✔] No duplicate records found" if duplicate_free else "[✘] Duplicate records found")


def test_no_null_values_in_mandatory_columns(input_location, input_file):
    file_path = os.path.join(input_location, input_file)
    df = pd.read_csv(file_path, sep='|', header=None)
    # Assuming FirstName, LastName, and Email are mandatory (indexes 0, 1, and 11)
    no_nulls = df[[0, 1, 11]].notnull().all().all()
    check.equal(no_nulls, True)
    print("[✔] No null values in mandatory columns" if no_nulls else "[✘] Null values found in mandatory columns")

def test_data_type_integrity(input_location, input_file):
    file_path = os.path.join(input_location, input_file)
    df = pd.read_csv(file_path, sep='|', header=None)
    # Assuming Salary is the 4th column (index 4) and should be numeric
    numeric = pd.to_numeric(df[4], errors='coerce').notnull().all()
    check.equal(numeric, True)
    print("[✔] Salary column data types are correct" if numeric else "[✘] Salary column contains non-numeric data")



def test_mongo_db_collection_exists(mongo_url, config):
    try:
        client = MongoClient(mongo_url)
        db_name = config['database']['name']
        collection_name = config['database']['collection']
        db = client[db_name]
        collection_exists = collection_name in db.list_collection_names()
        check.equal(collection_exists, True)
        print("[✔] MongoDB collection exists" if collection_exists else "[✘] MongoDB collection does not exist")
    except errors.ServerSelectionTimeoutError:
        check.equal(False, True)  # This will fail if the connection can't be established
        print("[✘] MongoDB connection failed")



def test_output_location_is_writable(output_location):
    writable = os.access(output_location, os.W_OK)
    check.equal(writable, True)
    print("[✔] Output location is writable" if writable else "[✘] Output location is not writable")

def test_mongo_db_url_is_reachable(mongo_url):
    try:
        client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
        client.server_info()  # Attempt to connect to MongoDB server
        reachable = True
    except errors.ServerSelectionTimeoutError:
        reachable = False
    check.equal(reachable, True)
    print("[✔] MongoDB URL is reachable" if reachable else "[✘] MongoDB URL is not reachable")



if __name__ == '__main__':
    pytest.main()
