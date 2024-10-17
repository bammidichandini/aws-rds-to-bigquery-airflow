import mysql.connector

mydb = mysql.connector.connect(
  host="database-1.crwiqeqyybhx.us-east-1.rds.amazonaws.com",
  user="admin",
  password="iBhubs1431#"
)

mydb_object = mydb.cursor()
mydb_object.execute("CREATE DATABASE RetailSalesDataset")
mydb_object.execute("SHOW DATABASES")
for i in mydb_object:
    print(i)