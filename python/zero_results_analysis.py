"""
Zero Results Analysis for Search Behavior Analytics
Author: Phong Trang
Description: Identifies content gaps through zero-result search analysis
"""

import pandas as pd
from databricks import sql
from collections import Counter
import re
from typing import List, Dict, Tuple

class ZeroResultsAnalyzer:
    """
    Analyzes search queries that return zero results
    
    Purpose:
    - Identify content gaps (what users want but can't find)
    - Detect spelling/typo patterns
    - Prioritize content creation opportunities
    - Track improvement over time
    """
    
    def __init__(self, spark_session):
        """
        Initialize analyzer with Spark session
        
        Args:
            spark_session: Active Databricks Spark session
        """
        self.spark = spark_session
    
    def load_zero_result_searches(self, start_date, end_date):
        """
        Load all searches that returned zero results
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: Zero-result search events
        """
        query = f"""
            SELECT 
                user_pseudo_id,
                event_date,
                event_timestamp,
                search_term,
                search_result_count,
                region,
                device_category
            FROM ga4_events
            WHERE event_category = 'global search'
              AND event_name = 'global_search_submit'
              AND search_result_count = 0
              AND event_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY event_date DESC
        """
        
        zero_results = self.spark.sql(query).toPandas()
        print(f"Loaded {len(zero_results):,} zero-result searches")
        return zero_results
    
    def analyze_top_zero_result_terms(self, zero_results_df, top_n=20):
        """
        Identify most common zero-result search terms
        
        Args:
            zero_results_df (pd.DataFrame): Zero-result searches
            top_n (int): Number of top terms to return
            
        Returns:
            pd.DataFrame: Top zero-result terms with frequencies
        """
        # Normalize search terms (lowercase, strip whitespace)
        zero_results_df['search_term_normalized'] = (
            zero_results_df['search_term']
            .str.lower()
            .str.strip()
        )
        
        # Count occurrences
        term_counts = zero_results_df.groupby('search_term_normalized').agg({
            'user_pseudo_id': 'count',
            'search_term': 'first'  # Keep original casing
        }).rename(columns={
            'user_pseudo_id': 'search_count',
            'search_term': 'original_term'
        }).sort_values('search_count', ascending=False)
        
        # Calculate percentage
        total_zero_searches = term_counts['search_count'].sum()
        term_counts['percentage'] = (
            term_counts['search_count'] / total_zero_searches * 100
        )
        
        return term_counts.head(top_n).reset_index()
    
    def detect_potential_typos(self, zero_results_df, successful_searches_df):
        """
        Identify likely typos by comparing to successful searches
        
        Args:
            zero_results_df (pd.DataFrame): Zero-result searches
            successful_searches_df (pd.DataFrame): Successful searches
            
        Returns:
            pd.DataFrame: Potential typos with suggested corrections
        """
        from difflib import get_close_matches
        
        # Get normalized terms
        zero_terms = zero_results_df['search_term'].str.lower().unique()
        successful_terms = successful_searches_df['search_term'].str.lower().unique()
        
        typo_suggestions = []
        
        for zero_term in zero_terms:
            # Find close matches (edit distance)
            matches = get_close_matches(
                zero_term, 
                successful_terms, 
                n=3, 
                cutoff=0.6
            )
            
            if matches:
                frequency = len(
                    zero_results_df[
                        zero_results_df['search_term'].str.lower() == zero_term
                    ]
                )
                
                typo_suggestions.append({
                    'zero_result_term': zero_term,
                    'suggested_correction': matches[0],
                    'other_suggestions': matches[1:] if len(matches) > 1 else None,
                    'frequency': frequency
                })
        
        typo_df = pd.DataFrame(typo_suggestions)
        return typo_df.sort_values('frequency', ascending=False)
    
    def analyze_by_region(self, zero_results_df):
        """
        Analyze zero-result patterns by region
        
        Args:
            zero_results_df (pd.DataFrame): Zero-result searches
            
        Returns:
            pd.DataFrame: Zero-result rates by region
        """
        regional_analysis = zero_results_df.groupby('region').agg({
            'search_term': 'count',
            'user_pseudo_id': 'nunique'
        }).rename(columns={
            'search_term': 'zero_result_count',
            'user_pseudo_id': 'unique_users'
        }).sort_values('zero_result_count', ascending=False)
        
        return regional_analysis.reset_index()
    
    def categorize_zero_results(self, zero_results_df):
        """
        Categorize zero-result searches into types
        
        Categories:
        - Product names (likely typos or unavailable products)
        - Features/specs (content gap)
        - Comparison queries (content gap)
        - General terms (too broad)
        
        Args:
            zero_results_df (pd.DataFrame): Zero-result searches
            
        Returns:
            pd.DataFrame: Categorized zero-result searches
        """
        def categorize_search(search_term):
            search_lower = search_term.lower()
            
            # Define patterns
            if any(word in search_lower for word in ['vs', 'versus', 'compare', 'or']):
                return 'Comparison Query'
            elif any(word in search_lower for word in ['price', 'cost', 'msrp', 'lease']):
                return 'Pricing Query'
            elif any(word in search_lower for word in ['mpg', 'horsepower', 'hp', 'range', 'battery']):
                return 'Feature/Spec Query'
            elif len(search_term.split()) == 1:
                return 'Single Word (Broad)'
            else:
                return 'Other'
        
        zero_results_df['category'] = zero_results_df['search_term'].apply(categorize_search)
        
        category_summary = zero_results_df.groupby('category').agg({
            'search_term': 'count',
            'user_pseudo_id': 'nunique'
        }).rename(columns={
            'search_term': 'search_count',
            'user_pseudo_id': 'unique_users'
        }).sort_values('search_count', ascending=False)
        
        return category_summary.reset_index()
    
    def calculate_zero_result_rate_trend(self, start_date, end_date):
        """
        Track zero-result rate over time to measure improvement
        
        Args:
            start_date (str): Start date YYYY-MM-DD
            end_date (str): End date YYYY-MM-DD
            
        Returns:
            pd.DataFrame: Daily zero-result rates
        """
        query = f"""
            SELECT 
                event_date,
                COUNT(*) as total_searches,
                SUM(CASE WHEN search_result_count = 0 THEN 1 ELSE 0 END) as zero_result_searches
            FROM ga4_events
            WHERE event_category = 'global search'
              AND event_name = 'global_search_submit'
              AND event_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY event_date
            ORDER BY event_date
        """
        
        trend_df = self.spark.sql(query).toPandas()
        trend_df['zero_result_rate'] = (
            trend_df['zero_result_searches'] / trend_df['total_searches']
        )
        
        return trend_df
    
    def generate_content_gap_report(self, start_date, end_date):
        """
        Generate comprehensive content gap analysis report
        
        Args:
            start_date (str): Start date YYYY-MM-DD
            end_date (str): End date YYYY-MM-DD
            
        Returns:
            dict: Complete zero-result analysis
        """
        print(f"Generating content gap report for {start_date} to {end_date}")
        
        # Load zero-result data
        zero_results = self.load_zero_result_searches(start_date, end_date)
        
        # Analyze top terms
        top_terms = self.analyze_top_zero_result_terms(zero_results, top_n=20)
        
        # Analyze by region
        regional_analysis = self.analyze_by_region(zero_results)
        
        # Categorize searches
        categories = self.categorize_zero_results(zero_results)
        
        # Calculate trend
        trend = self.calculate_zero_result_rate_trend(start_date, end_date)
        
        results = {
            'top_zero_result_terms': top_terms,
            'regional_analysis': regional_analysis,
            'category_breakdown': categories,
            'trend_over_time': trend,
            'summary': {
                'total_zero_result_searches': len(zero_results),
                'unique_zero_result_terms': zero_results['search_term'].nunique(),
                'affected_users': zero_results['user_pseudo_id'].nunique(),
                'avg_zero_result_rate': trend['zero_result_rate'].mean()
            }
        }
        
        return results


def main():
    """
    Example usage of ZeroResultsAnalyzer
    """
    from pyspark.sql import SparkSession
    from datetime import datetime, timedelta
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("ZeroResultsAnalysis").getOrCreate()
    
    # Initialize analyzer
    analyzer = ZeroResultsAnalyzer(spark)
    
    # Set date range
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    # Generate report
    report = analyzer.generate_content_gap_report(
        start_date=start_date,
        end_date=end_date
    )
    
    # Print results
    print("\n=== ZERO RESULTS SUMMARY ===")
    print(f"Total Zero-Result Searches: {report['summary']['total_zero_result_searches']:,}")
    print(f"Unique Terms: {report['summary']['unique_zero_result_terms']:,}")
    print(f"Affected Users: {report['summary']['affected_users']:,}")
    print(f"Average Zero-Result Rate: {report['summary']['avg_zero_result_rate']:.2%}")
    
    print("\n=== TOP 10 ZERO-RESULT SEARCHES (Content Gap Opportunities) ===")
    print(report['top_zero_result_terms'][['original_term', 'search_count', 'percentage']].head(10))
    
    print("\n=== CATEGORY BREAKDOWN ===")
    print(report['category_breakdown'])
    
    # Save results
    report['top_zero_result_terms'].to_csv('zero_results_top_terms.csv', index=False)
    report['regional_analysis'].to_csv('zero_results_by_region.csv', index=False)
    print("\nResults saved to CSV files")


if __name__ == "__main__":
    main()