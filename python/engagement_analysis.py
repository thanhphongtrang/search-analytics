"""
Engagement Analysis for Search Behavior Analytics
Author: Phong Trang
Description: Analyzes GA4 search events to calculate engagement metrics
"""

import pandas as pd
from databricks import sql
from datetime import datetime, timedelta
import numpy as np

class SearchEngagementAnalyzer:
    """
    Analyzes search engagement metrics from GA4 data
    
    Key Metrics:
    - Engagement Rate: Search result clicks / search sessions
    - Search Refinement Rate: Users modifying search terms
    - Average Results Returned per search
    - Click Position Analysis
    """
    
    def __init__(self, spark_session):
        """
        Initialize analyzer with Spark session
        
        Args:
            spark_session: Active Databricks Spark session
        """
        self.spark = spark_session
    
    def load_search_events(self, start_date, end_date):
        """
        Load GA4 search behavior data from Databricks
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: Search events with user interactions
        """
        query = f"""
            SELECT 
                user_pseudo_id,
                event_date,
                event_timestamp,
                search_term,
                search_result_count,
                event_name,
                event_label,
                event_category,
                click_position,
                session_id
            FROM ga4_events
            WHERE event_category = 'global search'
              AND event_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY user_pseudo_id, event_timestamp
        """
        
        search_events = self.spark.sql(query).toPandas()
        print(f"Loaded {len(search_events):,} search events")
        return search_events
    
    def calculate_engagement_metrics(self, search_events):
        """
        Calculate daily engagement metrics
        
        Args:
            search_events (pd.DataFrame): Raw GA4 search events
            
        Returns:
            pd.DataFrame: Aggregated engagement metrics by date
        """
        # Calculate key metrics by date
        engagement_metrics = search_events.groupby('event_date').agg({
            'user_pseudo_id': 'nunique',
            'search_term': 'count',
            'search_result_count': 'mean',
            'session_id': 'nunique'
        }).rename(columns={
            'user_pseudo_id': 'unique_users',
            'search_term': 'total_searches',
            'search_result_count': 'avg_results_returned',
            'session_id': 'unique_sessions'
        })
        
        # Calculate engagement rate (clicks / searches)
        searches_submitted = search_events[
            search_events['event_name'] == 'global_search_submit'
        ].groupby('event_date').size()
        
        clicks_on_results = search_events[
            search_events['event_name'] == 'global_search_click_result'
        ].groupby('event_date').size()
        
        engagement_metrics['searches_submitted'] = searches_submitted
        engagement_metrics['clicks_on_results'] = clicks_on_results
        engagement_metrics['engagement_rate'] = (
            engagement_metrics['clicks_on_results'] / 
            engagement_metrics['searches_submitted']
        ).fillna(0)
        
        # Calculate search refinement rate
        refinements = search_events[
            search_events['event_name'] == 'global_search_refinement'
        ].groupby('event_date').size()
        
        engagement_metrics['refinements'] = refinements.fillna(0)
        engagement_metrics['refinement_rate'] = (
            engagement_metrics['refinements'] / 
            engagement_metrics['searches_submitted']
        ).fillna(0)
        
        return engagement_metrics.reset_index()
    
    def analyze_click_positions(self, search_events):
        """
        Analyze which positions users click on search results
        
        Args:
            search_events (pd.DataFrame): Raw GA4 search events
            
        Returns:
            pd.DataFrame: Click distribution by position
        """
        clicks = search_events[
            search_events['event_name'] == 'global_search_click_result'
        ].copy()
        
        position_analysis = clicks.groupby('click_position').agg({
            'user_pseudo_id': 'count',
            'search_term': 'nunique'
        }).rename(columns={
            'user_pseudo_id': 'total_clicks',
            'search_term': 'unique_search_terms'
        })
        
        position_analysis['click_percentage'] = (
            position_analysis['total_clicks'] / 
            position_analysis['total_clicks'].sum() * 100
        )
        
        return position_analysis.sort_index()
    
    def calculate_search_to_conversion(self, search_events, orders_df):
        """
        Calculate search-to-order conversion funnel
        
        Args:
            search_events (pd.DataFrame): GA4 search events
            orders_df (pd.DataFrame): Order completion data from Snowflake
            
        Returns:
            dict: Conversion metrics
        """
        # Get unique users who searched
        searched_users = set(search_events['user_pseudo_id'].unique())
        
        # Get users who completed orders
        converted_users = set(orders_df['user_pseudo_id'].unique())
        
        # Users who searched AND converted
        search_to_conversion = searched_users.intersection(converted_users)
        
        conversion_rate = len(search_to_conversion) / len(searched_users) if searched_users else 0
        
        return {
            'total_users_searched': len(searched_users),
            'users_converted': len(search_to_conversion),
            'conversion_rate': conversion_rate,
            'conversion_rate_pct': f"{conversion_rate * 100:.2f}%"
        }
    
    def generate_engagement_report(self, start_date, end_date, orders_df=None):
        """
        Generate comprehensive engagement analysis report
        
        Args:
            start_date (str): Start date YYYY-MM-DD
            end_date (str): End date YYYY-MM-DD
            orders_df (pd.DataFrame, optional): Order data for conversion analysis
            
        Returns:
            dict: Complete engagement analysis results
        """
        print(f"Generating engagement report for {start_date} to {end_date}")
        
        # Load data
        search_events = self.load_search_events(start_date, end_date)
        
        # Calculate metrics
        engagement_metrics = self.calculate_engagement_metrics(search_events)
        click_positions = self.analyze_click_positions(search_events)
        
        results = {
            'daily_metrics': engagement_metrics,
            'click_position_analysis': click_positions,
            'summary': {
                'avg_engagement_rate': engagement_metrics['engagement_rate'].mean(),
                'avg_refinement_rate': engagement_metrics['refinement_rate'].mean(),
                'total_unique_users': engagement_metrics['unique_users'].sum(),
                'total_searches': engagement_metrics['total_searches'].sum()
            }
        }
        
        # Add conversion analysis if order data provided
        if orders_df is not None:
            conversion_metrics = self.calculate_search_to_conversion(
                search_events, orders_df
            )
            results['conversion_metrics'] = conversion_metrics
        
        return results


def main():
    """
    Example usage of SearchEngagementAnalyzer
    """
    from pyspark.sql import SparkSession
    
    # Initialize Spark session (Databricks)
    spark = SparkSession.builder.appName("SearchEngagement").getOrCreate()
    
    # Initialize analyzer
    analyzer = SearchEngagementAnalyzer(spark)
    
    # Set date range (last 30 days)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    # Load order data from Snowflake (optional)
    orders_df = spark.sql("""
        SELECT DISTINCT user_pseudo_id
        FROM snowflake_orders
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
    """).toPandas()
    
    # Generate report
    report = analyzer.generate_engagement_report(
        start_date=start_date,
        end_date=end_date,
        orders_df=orders_df
    )
    
    # Print summary
    print("\n=== ENGAGEMENT SUMMARY ===")
    print(f"Average Engagement Rate: {report['summary']['avg_engagement_rate']:.2%}")
    print(f"Average Refinement Rate: {report['summary']['avg_refinement_rate']:.2%}")
    print(f"Total Unique Users: {report['summary']['total_unique_users']:,}")
    print(f"Total Searches: {report['summary']['total_searches']:,}")
    
    if 'conversion_metrics' in report:
        print(f"\nSearch-to-Order Conversion: {report['conversion_metrics']['conversion_rate_pct']}")
    
    print("\n=== TOP 5 CLICK POSITIONS ===")
    print(report['click_position_analysis'].head())
    
    # Save results
    report['daily_metrics'].to_csv('engagement_metrics_daily.csv', index=False)
    print("\nResults saved to engagement_metrics_daily.csv")


if __name__ == "__main__":
    main()