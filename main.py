import etl
import time

def main():
    time.sleep(10)
    df=etl.read_csv()
    etl.transform_data(df)
    etl.load_data()

    # Add other code here

if __name__ == "__main__":
    main()

    