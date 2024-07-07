import time
import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, avg, max, min, lag
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName('FinancialDataProcessing').getOrCreate()

# Define the top-performing stocks you want to analyze
top_stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# Define the period
period1 = int(time.mktime(datetime.datetime(2023, 12, 1, 23, 59).timetuple()))
period2 = int(time.mktime(datetime.datetime(2024, 7, 1, 23, 59).timetuple()))
interval = '1d'  # 1wk, 1m

# Function to fetch data from Yahoo Finance and return a Spark DataFrame
def fetch_data(symbol):
    query = f'https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={period1}&period2={period2}&interval={interval}&events=history&includeAdjustedClose=true'
    pdf = pd.read_csv(query)
    pdf['Symbol'] = symbol
    return spark.createDataFrame(pdf)

# Fetch data for each stock and combine into a single DataFrame
dfs = [fetch_data(symbol) for symbol in top_stocks]
combined_df = dfs[0]
for df in dfs[1:]:
    combined_df = combined_df.union(df)

# Ensure 'Date' column is of date type
combined_df = combined_df.withColumn("Date", col("Date").cast("date"))

# Add year, month, and day columns
combined_df = combined_df.withColumn('Year', year(col('Date'))) \
                         .withColumn('Month', month(col('Date'))) \
                         .withColumn('Day', dayofmonth(col('Date')))

# Define window specification
window_spec = Window.partitionBy('Symbol').orderBy('Date')

# Calculate Simple Moving Average (SMA)
sma_window_spec = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-2, 0)
combined_df = combined_df.withColumn('SMA_3', avg(col('Close')).over(sma_window_spec))

# Calculate previous close price
combined_df = combined_df.withColumn('Prev_Close', lag('Close', 1).over(window_spec))

# Calculate daily return
combined_df = combined_df.withColumn('Daily_Return', (col('Close') - col('Prev_Close')) / col('Prev_Close') * 100)

# Calculate high-low spread
combined_df = combined_df.withColumn('High_Low_Spread', col('High') - col('Low'))

# Perform aggregations
aggregated_df = combined_df.groupBy('Symbol', 'Year', 'Month').agg(
    avg('Close').alias('Avg_Close'),
    max('Close').alias('Max_Close'),
    min('Close').alias('Min_Close')
)

#saving the df to csv
combined_df.write.csv('/home/kairo/Documents/Internship/csv/combined_df.csv',header=True)
aggregated_df.write.csv('/home/kairo/Documents/Internship/csv/aggregated_df.csv',header=True)

# Show result

combined_df.show()
aggregated_df.show()
