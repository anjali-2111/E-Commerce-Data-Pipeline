import random
import csv
from datetime import datetime,timedelta
import json
import boto3
import os

s3_client = boto3.client('s3')

# Define customer and product IDs (replace with your actual data)
customer_ids = ["CUST00001", "CUST00002", "CUST00003", "CUST00004", "CUST00005"]
product_ids = ["PROD00001", "PROD00002", "PROD00003", "PROD00004", "PROD00005"]

# Define number of transactions per day
transactions_per_day = 10

# Define product prices (replace with actual values from your product table)
product_prices = {
    "PROD00001": 799.99,
    "PROD00002": 499.99,
    "PROD00003": 79.99,
    "PROD00004": 19.99,
    "PROD00005": 49.99,
    "PROD00006": 99.99,
    "PROD00007": 149.99,
    "PROD00008": 199.99,
    "PROD00009": 14.99,
    "PROD00010": 19.99,
}

# Function to generate mock data for the CSV file

def generate_csv_data(num_rows):
    data = []
    for _ in range(num_rows):
        transaction_id = f"TXN{random.randint(100000000, 999999999)}"
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        price = product_prices[product_id]
        transaction_date = datetime.now().strftime('%Y-%m-%d')
        payment_type = random.choice(["Credit Card", "Debit Card", "Cash on Delivery"])
        status = random.choice(['Completed', 'Pending', 'Cancelled'])
        data.append([transaction_id, customer_id, product_id, price, transaction_date, payment_type, status])
    return data


# Function to write data to CSV file
def write_to_csv(data, filename):
    filepath = f"/tmp/{filename}"  # Save the file in /tmp directory
    with open(filepath, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['transaction_id', 'customer_id', 'product_id', 'price','transaction_date', 'payment_type', 'status'])
        writer.writerows(data)
    return filepath


def lambda_handler(event,context):
    num_rows = 20  # Number of rows in the CSV file
  
    csv_data = generate_csv_data(num_rows)
    date =datetime.now()
    filename = f"transactions_{date.strftime('%Y-%m-%d')}.csv"
    
    
    csv_filepath=write_to_csv(csv_data, filename)

    
    # Upload CSV file to S3 with Hive-style partitioning
    bucket = 'e-commerce122'
    s3_prefix = f"transactions/year={date.strftime('%Y')}/month={date.strftime('%m')}/day={date.strftime('%d')}/"
    s3_key = s3_prefix + filename
    s3_client.put_object(Body=open(csv_filepath, 'rb'), Bucket=bucket, Key=s3_key)


    return {
        'statuscode':200,
        'body':json.dumps('Data generated and saved to s3')
    }