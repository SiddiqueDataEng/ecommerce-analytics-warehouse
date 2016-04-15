"""
Unit tests for E-commerce ETL Pipeline
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ecommerce_etl import EcommerceETL


class TestEcommerceETL(unittest.TestCase):
    """Test E-commerce ETL functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            'batch_size': 1000,
            'snowflake_account': 'test_account',
            'database': 'test_db'
        }
        self.etl = EcommerceETL(self.config)
        
        # Create sample data
        self.sample_events = self._create_sample_events()
        self.sample_orders = self._create_sample_orders()
    
    def _create_sample_events(self) -> pd.DataFrame:
        """Create sample web events"""
        return pd.DataFrame({
            'event_id': [f'EVT-{i:04d}' for i in range(100)],
            'customer_id': [f'CUST-{i%10:03d}' for i in range(100)],
            'session_id': [f'SESS-{i%20:04d}' for i in range(100)],
            'event_timestamp': pd.date_range('2024-01-01', periods=100, freq='H'),
            'page_views': np.random.randint(1, 5, 100),
            'product_views': np.random.randint(0, 3, 100),
            'add_to_cart': np.random.randint(0, 2, 100),
            'checkout_started': np.random.randint(0, 2, 100),
            'purchase_completed': np.random.randint(0, 2, 100)
        })
    
    def _create_sample_orders(self) -> pd.DataFrame:
        """Create sample orders"""
        return pd.DataFrame({
            'order_id': [f'ORD-{i:04d}' for i in range(50)],
            'customer_id': [f'CUST-{i%10:03d}' for i in range(50)],
            'order_date': pd.date_range('2024-01-01', periods=50, freq='D'),
            'order_total': np.random.uniform(20, 500, 50).round(2),
            'order_status': ['completed'] * 50
        })
    
    def test_extract_web_events(self):
        """Test web events extraction"""
        events = self.etl.extract_web_events('2024-01-01', '2024-12-31')
        
        self.assertIsInstance(events, pd.DataFrame)
        self.assertGreater(len(events), 0)
        self.assertIn('event_id', events.columns)
        self.assertIn('customer_id', events.columns)
        self.assertIn('session_id', events.columns)
    
    def test_extract_orders(self):
        """Test orders extraction"""
        orders = self.etl.extract_orders('2024-01-01', '2024-12-31')
        
        self.assertIsInstance(orders, pd.DataFrame)
        self.assertGreater(len(orders), 0)
        self.assertIn('order_id', orders.columns)
        self.assertIn('customer_id', orders.columns)
        self.assertIn('order_total', orders.columns)
    
    def test_transform_customer_behavior(self):
        """Test customer behavior transformation"""
        behavior = self.etl.transform_customer_behavior(self.sample_events)
        
        self.assertIsInstance(behavior, pd.DataFrame)
        self.assertGreater(len(behavior), 0)
        self.assertIn('session_duration_seconds', behavior.columns)
        self.assertIn('converted', behavior.columns)
        self.assertIn('abandoned_cart', behavior.columns)
    
    def test_calculate_rfm_scores(self):
        """Test RFM score calculation"""
        rfm = self.etl.calculate_rfm_scores(self.sample_orders)
        
        self.assertIsInstance(rfm, pd.DataFrame)
        self.assertGreater(len(rfm), 0)
        
        # Check RFM columns exist
        self.assertIn('recency', rfm.columns)
        self.assertIn('frequency', rfm.columns)
        self.assertIn('monetary', rfm.columns)
        self.assertIn('r_score', rfm.columns)
        self.assertIn('f_score', rfm.columns)
        self.assertIn('m_score', rfm.columns)
        self.assertIn('segment', rfm.columns)
        
        # Check score ranges
        self.assertTrue((rfm['r_score'] >= 1).all())
        self.assertTrue((rfm['r_score'] <= 5).all())
        self.assertTrue((rfm['f_score'] >= 1).all())
        self.assertTrue((rfm['f_score'] <= 5).all())
    
    def test_rfm_segment_assignment(self):
        """Test RFM segment assignment logic"""
        # Test Champions segment
        row_champions = pd.Series({'r_score': 5, 'f_score': 5, 'm_score': 5})
        segment = self.etl._assign_rfm_segment(row_champions)
        self.assertEqual(segment, 'Champions')
        
        # Test Lost Customers segment
        row_lost = pd.Series({'r_score': 1, 'f_score': 1, 'm_score': 1})
        segment = self.etl._assign_rfm_segment(row_lost)
        self.assertEqual(segment, 'Lost Customers')
        
        # Test At Risk segment
        row_at_risk = pd.Series({'r_score': 1, 'f_score': 4, 'm_score': 4})
        segment = self.etl._assign_rfm_segment(row_at_risk)
        self.assertEqual(segment, 'At Risk')
    
    def test_analyze_product_affinity(self):
        """Test product affinity analysis"""
        # Create sample order items
        order_items = pd.DataFrame({
            'order_id': ['ORD-001', 'ORD-001', 'ORD-002', 'ORD-002', 'ORD-003'],
            'product_id': ['PROD-A', 'PROD-B', 'PROD-A', 'PROD-C', 'PROD-B']
        })
        
        affinity = self.etl.analyze_product_affinity(self.sample_orders, order_items)
        
        self.assertIsInstance(affinity, pd.DataFrame)
        
        if len(affinity) > 0:
            self.assertIn('product_a', affinity.columns)
            self.assertIn('product_b', affinity.columns)
            self.assertIn('co_occurrence_count', affinity.columns)
            self.assertIn('support', affinity.columns)
            
            # Check support is between 0 and 1
            self.assertTrue((affinity['support'] >= 0).all())
            self.assertTrue((affinity['support'] <= 1).all())
    
    def test_calculate_conversion_funnel(self):
        """Test conversion funnel calculation"""
        funnel = self.etl.calculate_conversion_funnel(self.sample_events)
        
        self.assertIsInstance(funnel, pd.DataFrame)
        self.assertEqual(len(funnel), 5)  # 5 stages
        
        # Check required columns
        self.assertIn('stage', funnel.columns)
        self.assertIn('sessions', funnel.columns)
        self.assertIn('conversion_rate', funnel.columns)
        self.assertIn('drop_off_rate', funnel.columns)
        
        # Check stages are in order
        expected_stages = ['visit', 'product_view', 'add_to_cart', 'checkout', 'purchase']
        actual_stages = funnel['stage'].tolist()
        self.assertEqual(actual_stages, expected_stages)
        
        # Check conversion rates decrease through funnel
        conversion_rates = funnel['conversion_rate'].tolist()
        for i in range(len(conversion_rates) - 1):
            self.assertGreaterEqual(conversion_rates[i], conversion_rates[i + 1])
    
    def test_analyze_cohorts(self):
        """Test cohort analysis"""
        cohorts = self.etl.analyze_cohorts(self.sample_orders)
        
        self.assertIsInstance(cohorts, pd.DataFrame)
        self.assertGreater(len(cohorts), 0)
        
        # Check retention rates are between 0 and 100
        self.assertTrue((cohorts >= 0).all().all())
        self.assertTrue((cohorts <= 100).all().all())
    
    def test_run_full_pipeline(self):
        """Test complete ETL pipeline"""
        results = self.etl.run_full_pipeline('2024-01-01', '2024-12-31')
        
        self.assertIsInstance(results, dict)
        
        # Check all expected keys exist
        self.assertIn('customer_behavior', results)
        self.assertIn('rfm_scores', results)
        self.assertIn('conversion_funnel', results)
        self.assertIn('cohort_retention', results)
        self.assertIn('product_affinity', results)
        self.assertIn('summary', results)
        
        # Check summary metrics
        summary = results['summary']
        self.assertIn('total_events', summary)
        self.assertIn('total_orders', summary)
        self.assertIn('unique_customers', summary)
        self.assertIn('total_revenue', summary)
        self.assertIn('avg_order_value', summary)
        
        # Check metrics are positive
        self.assertGreater(summary['total_events'], 0)
        self.assertGreater(summary['total_orders'], 0)
        self.assertGreater(summary['unique_customers'], 0)
        self.assertGreater(summary['total_revenue'], 0)


class TestDataQuality(unittest.TestCase):
    """Test data quality checks"""
    
    def setUp(self):
        self.config = {'batch_size': 1000}
        self.etl = EcommerceETL(self.config)
    
    def test_no_duplicate_events(self):
        """Test that events don't have duplicates"""
        events = self.etl.extract_web_events('2024-01-01', '2024-12-31')
        
        duplicate_count = events['event_id'].duplicated().sum()
        self.assertEqual(duplicate_count, 0, "Found duplicate event IDs")
    
    def test_no_duplicate_orders(self):
        """Test that orders don't have duplicates"""
        orders = self.etl.extract_orders('2024-01-01', '2024-12-31')
        
        duplicate_count = orders['order_id'].duplicated().sum()
        self.assertEqual(duplicate_count, 0, "Found duplicate order IDs")
    
    def test_order_totals_positive(self):
        """Test that order totals are positive"""
        orders = self.etl.extract_orders('2024-01-01', '2024-12-31')
        
        self.assertTrue((orders['order_total'] > 0).all(), "Found non-positive order totals")
    
    def test_event_timestamps_valid(self):
        """Test that event timestamps are valid"""
        events = self.etl.extract_web_events('2024-01-01', '2024-12-31')
        
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(events['event_timestamp']))
        self.assertFalse(events['event_timestamp'].isna().any())


class TestPerformance(unittest.TestCase):
    """Test performance characteristics"""
    
    def setUp(self):
        self.config = {'batch_size': 10000}
        self.etl = EcommerceETL(self.config)
    
    def test_large_dataset_processing(self):
        """Test processing of large datasets"""
        import time
        
        start_time = time.time()
        results = self.etl.run_full_pipeline('2024-01-01', '2024-12-31')
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        # Should complete in reasonable time (< 30 seconds for sample data)
        self.assertLess(processing_time, 30, 
                       f"Processing took too long: {processing_time:.2f} seconds")


if __name__ == '__main__':
    unittest.main()
