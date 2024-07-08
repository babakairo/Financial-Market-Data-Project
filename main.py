from helpers import YahooFinanceFetcher, DataFrameProcessor, FileSaver, initialize_spark, define_period

def main():
    # Initialize Spark session
    spark = initialize_spark()
    
    # Define the period
    period1, period2 = define_period()
    interval = '1d'  # 1wk, 1m
    
    # Define the top-performing stocks you want to analyze
    top_stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

    fetcher = YahooFinanceFetcher(spark, period1, period2, interval)
    
    # Fetch data for each stock and combine into a single DataFrame
    dfs = [fetcher.fetch_data(symbol) for symbol in top_stocks]
    combined_df = dfs[0]
    for df in dfs[1:]:
        combined_df = combined_df.union(df)

    processor = DataFrameProcessor(combined_df)
    
    # Process the combined DataFrame
    combined_df = processor.add_date_parts()
    combined_df = processor.calculate_sma()
    combined_df = processor.calculate_previous_close()
    combined_df = processor.calculate_daily_return()
    combined_df = processor.calculate_high_low_spread()
    
    # Perform aggregations
    aggregated_df = processor.perform_aggregations()
    
    # Save the processed DataFrames to CSV
    FileSaver.save_to_csv(combined_df, '/home/kairo/Documents/Internship/csv/combined_df.csv')
    FileSaver.save_to_csv(aggregated_df, '/home/kairo/Documents/Internship/csv/aggregated_df.csv')
    
    # Show results
    combined_df.show()
    aggregated_df.show()

if __name__ == "__main__":
    main()
