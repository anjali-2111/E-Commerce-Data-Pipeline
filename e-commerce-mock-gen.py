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


# # Function to generate mock data for a single customer
# def generate_customer(num_rows):
#     customer_data = []
#     for _ in range(num_rows):
#         customer_id = f"C{random.randint(10000, 99999)}"
#         first_name = f"First{random.randint(1, 100)}"
#         last_name = f"Last{random.randint(1, 100)}"
#         email = f"{first_name.lower()}.{last_name.lower()}@example.com"
#         membership_level = random.choice(["Gold", "Silver", "Bronze"])
#         customer_data.append({"customer_id": customer_id, "first_name": first_name, "last_name": last_name, "email": email, "membership_level": membership_level})
#     return customer_data

# def generate_product(num_rows):
#     products_data = []
#     for i in range(num_rows):
#         product_id = f"P{random.randint(10000,99999)}"
#         product_name = f"Widget {chr(65 + i)}"
#         category = random.choice(["Gadgets", "Electronics", "Clothing", "Books"])
#         price = round(random.uniform(10.0, 100.0), 2)
#         supplier_id = f"S{random.randint(10000,99999)}"
#         products_data.append({"product_id": product_id, "product_name": product_name, "category": category, "price": price, "supplier_id": supplier_id})
#     return products_data

# Function to generate mock data for the CSV file
# def generate_csv_data(num_rows, customers, products):
def generate_csv_data(num_rows):
    data = []
    for _ in range(num_rows):
        transaction_id = f"TXN{random.randint(100000000, 999999999)}"
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 10)
        # price = product["price"]
        price = product_prices[product_id]
        transaction_date = datetime.now().strftime('%Y-%m-%d')
        payment_type = random.choice(["Credit Card", "Debit Card", "Cash on Delivery"])
        status = random.choice(['Completed', 'Pending', 'Cancelled'])
        data.append([transaction_id, customer_id, product_id, quantity, price, transaction_date, payment_type, status])
    return data


# Function to write data to CSV file
def write_to_csv(data, filename):
    filepath = f"/tmp/{filename}"  # Save the file in /tmp directory
    with open(filepath, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['transaction_id', 'customer_id', 'product_id', 'quantity', 'price','transaction_date', 'payment_type', 'status'])
        writer.writerows(data)
    return filepath


# # Function to write data to CSV file for customers
# def write_to_csv_cust(data, filename1):
#     filepath1 = f"/tmp/{filename1}"  # Save the file in /tmp directory
#     with open(filepath1, mode='w', newline='') as file:
#         writer = csv.DictWriter(file, fieldnames=['customer_id', 'first_name', 'last_name', 'email', 'membership_level'])
#         writer.writeheader()
#         writer.writerows(data)
#     return filepath1

# # Function to write data to CSV file for products
# def write_to_csv_prod(data, filename2):
#     filepath2 = f"/tmp/{filename2}"  # Save the file in /tmp directory
#     with open(filepath2, mode='w', newline='') as file:
#         writer = csv.DictWriter(file, fieldnames=['product_id', 'product_name', 'category', 'price', 'supplier_id'])
#         writer.writeheader()
#         writer.writerows(data)
#     return filepath2


def lambda_handler(event,context):
    num_rows = 20  # Number of rows in the CSV file
    # cust_data = generate_customer(num_rows)
    # prod_data = generate_product(num_rows)
    # csv_data = generate_csv_data(num_rows, cust_data, prod_data)
    csv_data = generate_csv_data(num_rows)

    # date =  datetime.now().strftime('%Y-%m-%d') #datetime.strptime(event['date'], '%Y-%m-%d')
    date =datetime.now()
    filename = f"transactions_{date.strftime('%Y-%m-%d')}.csv"
    


    # Write data to CSV file
    
    # filename1 = f"customers_{date.strftime('%Y-%m-%d')}.csv"
    # filename2 = f"products_{date.strftime('%Y-%m-%d')}.csv"
    # cust_filepath = write_to_csv_cust(cust_data, filename1)
    # prod_filepath = write_to_csv_prod(prod_data, filename2)
    
    
    csv_filepath=write_to_csv(csv_data, filename)

    
    # Upload CSV file to S3 with Hive-style partitioning
    bucket = 'e-commerce122'
    s3_prefix = f"transactions/year={date.strftime('%Y')}/month={date.strftime('%m')}/day={date.strftime('%d')}/"
    s3_key = s3_prefix + filename
    s3_client.put_object(Body=open(csv_filepath, 'rb'), Bucket=bucket, Key=s3_key)

    # # customer and products csv 
    # s3_prefix = f"customers/year={date.strftime('%Y')}/month={date.strftime('%m')}/day={date.strftime('%d')}/"
    # s3_key = s3_prefix + filename1
    # s3_client.put_object(Body=open(cust_filepath, 'rb'), Bucket=bucket, Key=s3_key)
    
    
    # s3_prefix = f"products/year={date.strftime('%Y')}/month={date.strftime('%m')}/day={date.strftime('%d')}/"
    # s3_key = s3_prefix + filename2
    # s3_client.put_object(Body=open(prod_filepath, 'rb'), Bucket=bucket, Key=s3_key)

    return {
        'statuscode':200,
        'body':json.dumps('Data generated and saved to s3')
    }