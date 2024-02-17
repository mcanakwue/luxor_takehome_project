import requests
import jason
import psycopg2
import redis

# Make API call 
rep = requests.get("https://www.coingecko.com/api/documentations/v3")
# Response [403] was returned

# convert the jason file to python file
data = jason.load(rep)

#To store data in postgress database
conn =  psycopg2.connect("login-indetail")
cursor = conn.cursor()
for i in data:
    cursor.execute(INSERT INTO Table_1 ("column") , Value = ('value'))
conn.commit()
conn.close()

# calculate the average price in the past five minutes
#calculate the previous price
price = [i for i in data][-10:-6]
avg_price_previous = sum(price)/len(price)

#calculate the current price
price = [i for i in data][-5:-1]
avg_price_current = sum(price)/len(price)

# Publish an alert if the price different is greater than 2percent of the previous price
if abs(avg_price_current - avg_price_previous) > (2/100)*avg_price_previous:
   r = redis.Redis(host='localhost', port=6379, db=0)
   r.publish('alerts', alert_message)


# SQL script to create on postgress
   
create table table_1( timestamp TIMESTAMP,
                     col1 NUMERIC,
                     col2 NUMERIC,
                     col3 NUMERIC)
#creat Materialized view
CREATE MATERIALIZED VIEW daily_ohlc AS
SELECT timestamp,col1 ,col2 ,col3 
FROM  table_1

#How would you monitor the pipeline and ensure data integrity?
1, setup alarm metric to alert the user when certain threshold is exceded
2  Perform data validation to ensure that data quality and integrity
3  ensure that organization adhere to data governance

#How would you address scalability and performance?
1, Data cache or persistant to ensure the frequently accessed data is stored
2, data partitioning to ensure that data are processed in batches
3, ensure that the compute resources can be scaled when necessary

#Trade-offs you had to choose when doing this challenge (the things you would do different with more time and resources)
1, I will use more distributed architecture with an in memory storage like databricks for faster processing
2, 

