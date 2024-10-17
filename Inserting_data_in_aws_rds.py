import mysql.connector
import zipfile
import os
import pandas as pd

mydb = mysql.connector.connect(
  host="database-1.crwiqeqyybhx.us-east-1.rds.amazonaws.com",
  user="admin",
  password="iBhubs1431#",
  database = "RetailSalesDataset"
)

mydb_object = mydb.cursor()

# Path to the zip file
zip_archive_file_path = '/Users/chandinibammidi/Downloads/archive.zip'

# Directory to extract the contents
extract_dir_path = '/Users/chandinibammidi/Downloads/'

# Extract the ZIP file
with zipfile.ZipFile(zip_archive_file_path, 'r') as zip_ref:
    zip_ref.extractall(extract_dir_path)

print("Files extracted to", extract_dir_path)

csv_file_path = os.path.join(extract_dir_path,'retail_sales_dataset.csv')
df = pd.read_csv(csv_file_path)

mydb_object.execute('''
                    CREATE TABLE IF NOT EXISTS sales_data(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    transaction_id INT,
                    date DATE,
                    customer_id INT,
                    gender ENUM('Male', 'Female', 'Other') NOT NULL,
                    age TINYINT UNSIGNED NOT NULL,
                    product_category VARCHAR(255),
                    quantity INT, 
                    price_per_unit INT,
                    total_amount INT,
                    CONSTRAINT check_age CHECK (age BETWEEN 0 AND 100)
                    )
                    ''')


mydb_object.execute("SHOW TABLES")
for tables in mydb_object:
    print(tables[0])

for i, row in df.iterrows():
    sql = "INSERT INTO sales_data(transaction_id, date, customer_id, gender, age, product_category, quantity, price_per_unit, total_amount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mydb_object.execute(sql,tuple(row))

mydb.commit()
mydb_object.close()
mydb.close()