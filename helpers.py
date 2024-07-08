import time
import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, avg, max, min, lag
from pyspark.sql.window import Window

class YahooFinanceFetcher:
    def __init__(self, spark_session, period1, period2, interval='1d'):
        self.spark = spark_session
        self.period1 = period1
        self.period2 = period2
        self.interval = interval

    def fetch_data(self, symbol):
        query = f'https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={self.period1}&period2={self.period2}&interval={self.interval}&events=history&includeAdjustedClose=true'
        pdf = pd.read_csv(query)
        pdf['Symbol'] = symbol
        return self.spark.createDataFrame(pdf)

class DataFrameProcessor:
    def __init__(self, df):
        self.df = df

    def add_date_parts(self):
        self.df = self.df.withColumn("Date", col("Date").cast("date"))
        self.df = self.df.withColumn('Year', year(col('Date'))) \
                         .withColumn('Month', month(col('Date'))) \
                         .withColumn('Day', dayofmonth(col('Date')))
        return self.df

    def calculate_sma(self, window_size=3):
        sma_window_spec = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-window_size + 1, 0)
        self.df = self.df.withColumn(f'SMA_{window_size}', avg(col('Close')).over(sma_window_spec))
        return self.df

    def calculate_previous_close(self):
        window_spec = Window.partitionBy('Symbol').orderBy('Date')
        self.df = self.df.withColumn('Prev_Close', lag('Close', 1).over(window_spec))
        return self.df

    def calculate_daily_return(self):
        self.df = self.df.withColumn('Daily_Return', (col('Close') - col('Prev_Close')) / col('Prev_Close') * 100)
        return self.df

    def calculate_high_low_spread(self):
        self.df = self.df.withColumn('High_Low_Spread', col('High') - col('Low'))
        return self.df

    def perform_aggregations(self):
        aggregated_df = self.df.groupBy('Symbol', 'Year', 'Month').agg(
            avg('Close').alias('Avg_Close'),
            max('Close').alias('Max_Close'),
            min('Close').alias('Min_Close')
        )
        return aggregated_df

class FileSaver:
    @staticmethod
    def save_to_csv(df, path, header=True):
        df.write.csv(path, header=header)

def initialize_spark():
    return SparkSession.builder.appName('FinancialDataProcessing').getOrCreate()

def define_period():
    period1 = int(time.mktime(datetime.datetime(2023, 12, 1, 23, 59).timetuple()))
    period2 = int(time.mktime(datetime.datetime(2024, 7, 1, 23, 59).timetuple()))
    return period1, period2
