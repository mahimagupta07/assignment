# ETL Pipeline Project

This project implements an ETL (Extract, Transform, Load) pipeline for processing member data. It reads data from a CSV file, transforms it, and loads it into a MongoDB database.

## Project Structure

- `config.yaml`: Configuration file for the project
- `etl.py`: Contains the ETL logic
- `main.py`: Entry point of the application
- `test_etl.py`: Test suite for the ETL process

## Prerequisites

- Python 3.8+
- Docker and Docker Compose

## Configuration

The `config.yaml` file contains all the necessary configuration for the project:

- MongoDB connection details
- Database and collection names
- Input and output file paths

## Running the Application

1. Build the Docker image:
   ```
   docker-compose build
   ```

2. Run the application:
   ```
   docker-compose up
   ```

## Running Tests

To run the test suite:

```
docker-compose run app pytest test_etl.py
```

## Input Data Format

The input CSV file should have the following columns:
FirstName, LastName, Company, BirthDate, Salary, Address, Suburb, State, Post, Phone, Mobile, Email

## Output

The transformed data will be saved as a JSON file in the output directory and loaded into the specified MongoDB collection.

## Notes

- Make sure your input CSV file is placed in the correct location as specified in `config.yaml`.
- The application will create a `skipped_rows.csv` file for any rows with null values.