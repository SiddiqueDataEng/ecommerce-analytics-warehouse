"""
E-commerce Analytics ETL Pipeline
Processes customer behavior, orders, and product data for analytics
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import logging
from dataclasses import dataclass
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class CustomerSegment:
    """Customer segment definition"""
    segment_id: str
    segment_name: str
    rfm_score: str
    description: str


class EcommerceETL:
    """Main ETL pipeline for e-commerce analytics"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.batch_size = config.get('batch_size', 10000)
    
    def extract_web_events(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Extract web analytics events
        
        Args:
            start_date: Start date for extraction
            end_date: End date for extraction
        
        Returns:
            DataFrame with web events
        """
        try:
            logger.info(f"Extracting web events from {start_date} to {end_date}")
            
            # In production, this would connect to web analytics API
            # For demo, generate sample data
            events = self._generate_sample_web_events(start_date, end_date)
            
            logger.info(f"Extracted {len(events)} web events")
            return events
        
        except Exception as e:
            logger.error(f"Error extracting web events: {e}")
            raise
    
    def extract_orders(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Extract order data from transactional database"""
        try:
            logger.info(f"Extracting orders from {start_date} to {end_date}")
            
            # In production, query from order database
            orders = self._generate_sample_orders(start_date, end_date)
            
            logger.info(f"Extracted {len(orders)} orders")
            return orders
        
        except Exception as e:
            logger.error(f"Error extracting orders: {e}")
            raise
    
    def transform_customer_behavior(self, events: pd.DataFrame) -> pd.DataFrame:
        """
        Transform web events into customer behavior metrics
        
        Args:
            events: Raw web events
        
        Returns:
            Transformed customer behavior data
        """
        try:
            logger.info("Transforming customer behavior data")
            
            # Aggregate by customer and session
            behavior = events.groupby(['customer_id', 'session_id']).agg({
                'event_timestamp': ['min', 'max', 'count'],
                'page_views': 'sum',
                'product_views': 'sum',
                'add_to_cart': 'sum',
                'checkout_started': 'sum',
                'purchase_completed': 'sum'
            }).reset_index()
            
            # Flatten column names
            behavior.columns = ['_'.join(col).strip('_') for col in behavior.columns.values]
            
            # Calculate session duration
            behavior['session_duration_seconds'] = (
                behavior['event_timestamp_max'] - behavior['event_timestamp_min']
            ).dt.total_seconds()
            
            # Calculate conversion flags
            behavior['converted'] = (behavior['purchase_completed_sum'] > 0).astype(int)
            behavior['abandoned_cart'] = (
                (behavior['add_to_cart_sum'] > 0) & 
                (behavior['purchase_completed_sum'] == 0)
            ).astype(int)
            
            logger.info(f"Transformed {len(behavior)} customer sessions")
            return behavior
        
        except Exception as e:
            logger.error(f"Error transforming customer behavior: {e}")
            raise
    
    def calculate_rfm_scores(self, orders: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate RFM (Recency, Frequency, Monetary) scores
        
        Args:
            orders: Order data
        
        Returns:
            DataFrame with RFM scores per customer
        """
        try:
            logger.info("Calculating RFM scores")
            
            current_date = pd.Timestamp.now()
            
            # Calculate RFM metrics
            rfm = orders.groupby('customer_id').agg({
                'order_date': lambda x: (current_date - x.max()).days,  # Recency
                'order_id': 'count',  # Frequency
                'order_total': 'sum'  # Monetary
            }).reset_index()
            
            rfm.columns = ['customer_id', 'recency', 'frequency', 'monetary']
            
            # Calculate RFM scores (1-5 scale)
            rfm['r_score'] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1], duplicates='drop')
            rfm['f_score'] = pd.qcut(rfm['frequency'], 5, labels=[1, 2, 3, 4, 5], duplicates='drop')
            rfm['m_score'] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5], duplicates='drop')
            
            # Create RFM segment
            rfm['rfm_score'] = (
                rfm['r_score'].astype(str) + 
                rfm['f_score'].astype(str) + 
                rfm['m_score'].astype(str)
            )
            
            # Assign segment names
            rfm['segment'] = rfm.apply(self._assign_rfm_segment, axis=1)
            
            logger.info(f"Calculated RFM scores for {len(rfm)} customers")
            return rfm
        
        except Exception as e:
            logger.error(f"Error calculating RFM scores: {e}")
            raise
    
    def _assign_rfm_segment(self, row) -> str:
        """Assign customer segment based on RFM scores"""
        r, f, m = int(row['r_score']), int(row['f_score']), int(row['m_score'])
        
        if r >= 4 and f >= 4 and m >= 4:
            return 'Champions'
        elif r >= 3 and f >= 3 and m >= 3:
            return 'Loyal Customers'
        elif r >= 4 and f <= 2:
            return 'New Customers'
        elif r <= 2 and f >= 3:
            return 'At Risk'
        elif r <= 2 and f <= 2:
            return 'Lost Customers'
        elif r >= 3 and f <= 2:
            return 'Potential Loyalists'
        else:
            return 'Regular Customers'
    
    def analyze_product_affinity(self, orders: pd.DataFrame, 
                                 order_items: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze product affinity (products frequently bought together)
        
        Args:
            orders: Order data
            order_items: Order line items
        
        Returns:
            Product affinity matrix
        """
        try:
            logger.info("Analyzing product affinity")
            
            # Create product pairs from same orders
            pairs = []
            
            for order_id in order_items['order_id'].unique():
                products = order_items[order_items['order_id'] == order_id]['product_id'].tolist()
                
                # Generate all pairs
                for i in range(len(products)):
                    for j in range(i + 1, len(products)):
                        pairs.append({
                            'product_a': products[i],
                            'product_b': products[j],
                            'order_id': order_id
                        })
            
            if not pairs:
                return pd.DataFrame()
            
            affinity_df = pd.DataFrame(pairs)
            
            # Calculate affinity metrics
            affinity = affinity_df.groupby(['product_a', 'product_b']).agg({
                'order_id': 'count'
            }).reset_index()
            
            affinity.columns = ['product_a', 'product_b', 'co_occurrence_count']
            
            # Calculate support (percentage of orders containing both products)
            total_orders = len(orders)
            affinity['support'] = affinity['co_occurrence_count'] / total_orders
            
            # Sort by co-occurrence
            affinity = affinity.sort_values('co_occurrence_count', ascending=False)
            
            logger.info(f"Found {len(affinity)} product affinity pairs")
            return affinity
        
        except Exception as e:
            logger.error(f"Error analyzing product affinity: {e}")
            raise
    
    def calculate_conversion_funnel(self, events: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate conversion funnel metrics
        
        Args:
            events: Web events data
        
        Returns:
            Funnel metrics by stage
        """
        try:
            logger.info("Calculating conversion funnel")
            
            # Define funnel stages
            funnel_stages = {
                'visit': events['session_id'].nunique(),
                'product_view': events[events['product_views'] > 0]['session_id'].nunique(),
                'add_to_cart': events[events['add_to_cart'] > 0]['session_id'].nunique(),
                'checkout': events[events['checkout_started'] > 0]['session_id'].nunique(),
                'purchase': events[events['purchase_completed'] > 0]['session_id'].nunique()
            }
            
            # Create funnel DataFrame
            funnel = pd.DataFrame([
                {'stage': stage, 'sessions': count, 'stage_order': i}
                for i, (stage, count) in enumerate(funnel_stages.items(), 1)
            ])
            
            # Calculate conversion rates
            funnel['conversion_rate'] = (funnel['sessions'] / funnel['sessions'].iloc[0] * 100)
            funnel['drop_off_rate'] = 100 - funnel['conversion_rate']
            
            # Calculate stage-to-stage conversion
            funnel['stage_conversion'] = (
                funnel['sessions'] / funnel['sessions'].shift(1) * 100
            ).fillna(100)
            
            logger.info("Conversion funnel calculated")
            return funnel
        
        except Exception as e:
            logger.error(f"Error calculating conversion funnel: {e}")
            raise
    
    def analyze_cohorts(self, orders: pd.DataFrame) -> pd.DataFrame:
        """
        Perform cohort analysis based on first purchase date
        
        Args:
            orders: Order data
        
        Returns:
            Cohort retention matrix
        """
        try:
            logger.info("Performing cohort analysis")
            
            # Get first purchase date for each customer
            orders['order_month'] = orders['order_date'].dt.to_period('M')
            
            cohorts = orders.groupby('customer_id').agg({
                'order_date': 'min',
                'order_month': 'min'
            }).reset_index()
            
            cohorts.columns = ['customer_id', 'first_purchase_date', 'cohort_month']
            
            # Merge back to orders
            orders_with_cohort = orders.merge(cohorts, on='customer_id')
            
            # Calculate months since first purchase
            orders_with_cohort['months_since_first'] = (
                orders_with_cohort['order_month'] - 
                orders_with_cohort['cohort_month']
            ).apply(lambda x: x.n)
            
            # Create cohort matrix
            cohort_data = orders_with_cohort.groupby([
                'cohort_month', 'months_since_first'
            ])['customer_id'].nunique().reset_index()
            
            cohort_matrix = cohort_data.pivot(
                index='cohort_month',
                columns='months_since_first',
                values='customer_id'
            )
            
            # Calculate retention rates
            cohort_sizes = cohort_matrix.iloc[:, 0]
            retention = cohort_matrix.divide(cohort_sizes, axis=0) * 100
            
            logger.info(f"Cohort analysis complete for {len(cohort_matrix)} cohorts")
            return retention
        
        except Exception as e:
            logger.error(f"Error in cohort analysis: {e}")
            raise
    
    def _generate_sample_web_events(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Generate sample web events for demo"""
        np.random.seed(42)
        
        n_events = 10000
        n_customers = 500
        n_sessions = 2000
        
        events = pd.DataFrame({
            'event_id': [f'EVT-{i:06d}' for i in range(n_events)],
            'customer_id': [f'CUST-{np.random.randint(1, n_customers):04d}' for _ in range(n_events)],
            'session_id': [f'SESS-{np.random.randint(1, n_sessions):05d}' for _ in range(n_events)],
            'event_timestamp': pd.date_range(start=start_date, end=end_date, periods=n_events),
            'page_views': np.random.randint(0, 5, n_events),
            'product_views': np.random.randint(0, 3, n_events),
            'add_to_cart': np.random.randint(0, 2, n_events),
            'checkout_started': np.random.randint(0, 2, n_events),
            'purchase_completed': np.random.randint(0, 2, n_events)
        })
        
        return events
    
    def _generate_sample_orders(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Generate sample orders for demo"""
        np.random.seed(42)
        
        n_orders = 2000
        n_customers = 500
        
        orders = pd.DataFrame({
            'order_id': [f'ORD-{i:06d}' for i in range(n_orders)],
            'customer_id': [f'CUST-{np.random.randint(1, n_customers):04d}' for _ in range(n_orders)],
            'order_date': pd.date_range(start=start_date, end=end_date, periods=n_orders),
            'order_total': np.random.uniform(20, 500, n_orders).round(2),
            'order_status': np.random.choice(['completed', 'pending', 'cancelled'], n_orders, p=[0.85, 0.10, 0.05])
        })
        
        return orders
    
    def run_full_pipeline(self, start_date: str, end_date: str) -> Dict:
        """
        Run complete ETL pipeline
        
        Args:
            start_date: Start date for processing
            end_date: End date for processing
        
        Returns:
            Dictionary with all analytics results
        """
        try:
            logger.info("Starting full ETL pipeline")
            
            # Extract
            events = self.extract_web_events(start_date, end_date)
            orders = self.extract_orders(start_date, end_date)
            
            # Transform
            customer_behavior = self.transform_customer_behavior(events)
            rfm_scores = self.calculate_rfm_scores(orders)
            funnel = self.calculate_conversion_funnel(events)
            cohorts = self.analyze_cohorts(orders)
            
            # Generate sample order items for affinity analysis
            order_items = self._generate_sample_order_items(orders)
            affinity = self.analyze_product_affinity(orders, order_items)
            
            results = {
                'customer_behavior': customer_behavior,
                'rfm_scores': rfm_scores,
                'conversion_funnel': funnel,
                'cohort_retention': cohorts,
                'product_affinity': affinity,
                'summary': {
                    'total_events': len(events),
                    'total_orders': len(orders),
                    'unique_customers': orders['customer_id'].nunique(),
                    'total_revenue': orders['order_total'].sum(),
                    'avg_order_value': orders['order_total'].mean()
                }
            }
            
            logger.info("ETL pipeline completed successfully")
            return results
        
        except Exception as e:
            logger.error(f"Error in ETL pipeline: {e}")
            raise
    
    def _generate_sample_order_items(self, orders: pd.DataFrame) -> pd.DataFrame:
        """Generate sample order items"""
        np.random.seed(42)
        
        items = []
        for order_id in orders['order_id']:
            n_items = np.random.randint(1, 5)
            for _ in range(n_items):
                items.append({
                    'order_id': order_id,
                    'product_id': f'PROD-{np.random.randint(1, 100):03d}',
                    'quantity': np.random.randint(1, 4),
                    'unit_price': np.random.uniform(10, 200)
                })
        
        return pd.DataFrame(items)


if __name__ == "__main__":
    # Example usage
    config = {
        'batch_size': 10000,
        'snowflake_account': 'your_account',
        'database': 'ecommerce_analytics'
    }
    
    etl = EcommerceETL(config)
    
    # Run pipeline
    results = etl.run_full_pipeline(
        start_date='2024-01-01',
        end_date='2024-12-31'
    )
    
    print("\n" + "="*50)
    print("E-COMMERCE ANALYTICS SUMMARY")
    print("="*50)
    
    summary = results['summary']
    print(f"\nTotal Events: {summary['total_events']:,}")
    print(f"Total Orders: {summary['total_orders']:,}")
    print(f"Unique Customers: {summary['unique_customers']:,}")
    print(f"Total Revenue: ${summary['total_revenue']:,.2f}")
    print(f"Average Order Value: ${summary['avg_order_value']:.2f}")
    
    print("\n" + "="*50)
    print("RFM SEGMENTS")
    print("="*50)
    print(results['rfm_scores']['segment'].value_counts())
    
    print("\n" + "="*50)
    print("CONVERSION FUNNEL")
    print("="*50)
    print(results['conversion_funnel'][['stage', 'sessions', 'conversion_rate']])
