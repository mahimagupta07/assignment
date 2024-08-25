import pandas as pd
import datetime
from pymongo import MongoClient, errors
import json
import yaml
    
def read_csv():
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    filepath = config['file']['input']    
    cols = ['FirstName','LastName','Company','BirthDate','Salary','Address','Suburb','State','Post','Phone','Mobile','Email']
    df = pd.read_csv(filepath, sep='|', names=cols)
    df = df.convert_dtypes()
    return df


def transform_data(df):
    # Log rows with NaNs
    selected_rows=df[df.isnull().any(axis=1)].reset_index()
    selected_rows.to_csv('skipped_rows.csv',index=False)

    # Selecting the non null values
    df = df.dropna()

    
    ## Step 1: Date conversion
    def format_date(value):
        # Convert the integer to string
        s = str(value)
        # Ensure all dates have same digits with zero padding
        s = s.zfill(8)
        # Slicing the year from last 4 characters, month and day too
        year = s[4:]
        month = s[2:4]
        day = s[0:2]
        # converting to datetime object and formatting the string
        return pd.to_datetime(f'{day}/{month}/{year}',format='%d/%m/%Y') 

    # Apply the formatting function
    df['BirthDate'] = df['BirthDate'].apply(format_date)

    ## Step 2: Removing leading/trailing spaces and merge FirstName and lastName to FullName
    df['FirstName']=df['FirstName'].str.strip()
    df['LastName']=df['LastName'].str.strip()
    df['FullName'] = df['FirstName']+ ' ' +df['LastName']

    
    ## Step 3: Age calculation
    reference_date = datetime.datetime(2024, 3, 1)

    df['Age_in_days'] = (reference_date - df['BirthDate']).dt.days
    df['Age']= df['Age_in_days']/365.25
    df['Age']= df['Age'].round()
    del df['Age_in_days']

    # Step 4: Add SalaryBucket column
    df['Salary'] = df['Salary'].round(2).astype(float) #rounding to nearest cents
    df['SalaryBucket'] = pd.cut(df['Salary'], bins=[0, 50000, 100000, float('inf')],labels=['A', 'B', 'C'])

    # Step 5: Dollar sign and commas added to salary col
    df['Salary']=df['Salary'].apply(lambda x: '${:,.2f}'.format(x))

    # Step 6: Formatting birthdate to DD/MM/YYYY format
    df['BirthDate']=df['BirthDate'].dt.strftime('%d/%m/%Y')

    # Step 7: Nested Entity Class for Address
    class Address:
        def __init__(self, Address, Suburb, State, Post):
            self.street = Address
            self.suburb = Suburb
            self.state = State
            self.post_code = Post

        def __repr__(self):
            return f"{self.street}, {self.suburb}, {self.state} {self.post_code}"

    df['Address'] = df.apply(lambda row: Address(row['Address'], row['Suburb'], row['State'], row['Post']), axis=1)

    # Drop columns if not needed anymore
    df.drop(['FirstName','LastName','Suburb', 'State', 'Post'], axis=1, inplace=True)

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    filepath_output = config['file']['output'] 
    df.to_json(filepath_output,orient='records')

        
                

def load_data():
    # Base MongoDB URI without authentication
    # Load configuration from config.yaml
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    base_uri = config['mongo']['url']

    # Extract username and password
    username = config['mongo']['username']
    password = config['mongo']['password']
    db_name = config['database']['name']
    db_collec = config['database']['collection']
    filepath = config['file']['output']
    
    # Check if both username and password are None
    if username is None and password is None:
        connection_url = base_uri
    
    # Reconstruct the URI with authentication details if provided
    if username is not None and password is not None:
        # Construct URI with username and password
        connection_url= f'mongodb://{username}:{password}@{base_uri[len("mongodb://"):]}' 
    try:
        client = MongoClient(connection_url)
        # Access a specific database
        db = client[db_name]
        collection = db[db_collec]

        with open(filepath) as file:
            file_data = json.load(file)

        if isinstance(file_data, list):
            collection.insert_many(file_data)
        else:
            collection.insert_one(file_data)

        print("Data inserted successfully!")

        print(f"Collection '{collection.name}' created or accessed successfully.")

    except errors.ConnectionError as e:
        print(f"Connection failed: {e}")


    