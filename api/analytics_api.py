"""
E-commerce Analytics REST API
Provides endpoints for customer analytics, RFM, conversion funnel, and cohort analysis
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.ecommerce_etl import EcommerceETL

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize ETL
config = {
    'batch_size': 10000,
    'snowflake_account': os.getenv('SNOWFLAKE_ACCOUNT', 'your_account'),
    'database': 'ecommerce_analytics'
}
etl = EcommerceETL(config)


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'E-commerce Analytics API',
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/v1/analytics/summary', methods=['GET'])
def get_analytics_summary():
    """
    Get overall analytics summary
    
    Query params:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    """
    try:
        start_date = request.args.get('start_date', '2024-01-01')
        end_date = request.args.get('end_date', '2024-12-31')
        
        # Run ETL pipeline
        results = etl.run_full_pipeline(start_date, end_date)
        
        return jsonify({
            'success': True,
            'summary': results['summary'],
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error getting analytics summary: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v1/customers/rfm', methods=['GET'])
def get_rfm_analysis():
    """
    Get RFM (Recency, Frequency, Monetary) analysis
    
    Query params:
        start_date: Start date
        end_date: End date
        segment: Filter by segment (optional)
    """
    try:
        start_date = request.args.get('start_date', '2024-01-01')
        end_date = request.args.get('end_date', '2024-12-31')
        segment_filter = request.args.get('segment')
        
        # Extract orders and calculate RFM
        orders = etl.extract_orders(start_date, end_date)
        rfm_scores = etl.calculate_rfm_scores(orders)
        
        # Filter by segment if provided
        if segment_filter:
            rfm_scores = rfm_scores[rfm_scores['segment'] == segment_filter]
        
        # Calculate segment distribution
        segment_dist = rfm_scores['segment'].value_counts().to_dict()
        
        # Get top customers by monetary value
        top_customers = rfm_scores.nlargest(10, 'monetary')[
            ['customer_id', 'recency', 'frequency', 'monetary', 'segment']
        ].to_dict(orient='records')
        
        return jsonify({
            'success': True,
            'segment_distribution': segment_dist,
            'top_customers': top_customers,
            'total_customers': len(rfm_scores),
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error in RFM analysis: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v1/customers/segments', methods=['GET'])
def get_customer_segments():
    """Get customer segment definitions and counts"""
    try:
        start_date = request.args.get('start_date', '2024-01-01')
        end_date = request.args.get('end_date', '2024-12-31')
        
        orders = etl.extract_orders(start_date, end_date)
        rfm_scores = etl.calculate_rfm_scores(orders)
        
        # Segment definitions
        segments = {
            'Champions': {
                'description': 'Best customers with high RFM scores',
                'count': len(rfm_scores[rfm_scores['segment'] == 'Champions']),
                'avg_monetary': float(rfm_scores[rfm_scores['segment'] == 'Champions']['monetary'].mean())
            },
            'Loyal Customers': {
                'description': 'Regular buyers with good engagement',
                'count': len(rfm_scores[rfm_scores['segment'] == 'Loyal Customers']),
                'avg_monetary': float(rfm_scores[rfm_scores['segment'] == 'Loyal Customers']['monetary'].mean())
            },
            'At Risk': {
                'description': 'Previously good customers who haven\'t purchased recently',
                'count': len(rfm_scores[rfm_scores['segment'] == 'At Risk']),
                'avg_monetary': float(rfm_scores[rfm_scores['segment'] == 'At Risk']['monetary'].mean())
            },
            'Lost Customers': {
                'description': 'Haven\'t purchased in a long time',
                'count': len(rfm_scores[rfm_scores['segment'] == 'Lost Customers']),
                'avg_monetary': float(rfm_scores[rfm_scores['segment'] == 'Lost Customers']['monetary'].mean())
            }
        }
        
        return jsonify({
            'success': True,
            'segments': segments,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error getting customer segments: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v1/conversion/funnel', methods=['GET'])
def get_conversion_funnel():
    """
    Get conversion funnel analysis
    
    Query params:
        start_date: Start date
        end_date: End date
    """
    try:
        start_date = request.args.get('start_date', '2024-01-01')
        end_date = request.args.get('end_date', '2024-12-31')
        
        # Extract events and calculate funnel
        events = etl.extract_web_events(start_date, end_date)
        funnel = etl.calculate_conversion_funnel(events)
        
        # Convert to dict for JSON response
        funnel_data = funnel.to_dict(orient='records')
        
        # Calculate overall conversion rate
        overall_conversion = (
            funnel[funnel['stage'] == 'purchase']['sessions'].iloc[0] /
            funnel[funnel['stage'] == 'visit']['sessions'].iloc[0] * 100
        )
        
        return jsonify({
            'success': True,
            'funnel': funnel_data,
            'overall_conversion_rate': round(float(overall_conversion), 2),
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error calculating conversion funnel: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v1/products/affinity', methods=['GET'])
def get_product_affinity():
    """
    Get product affinity analysis (products bought together)
    
    Query params:
        start_date: Start date
        end_date: End date
        min_support: Minimum support threshold (default: 0.01)
        limit: Number of results to return (default: 20)
    """
    try:
        start_date = request.args.get('start_date', '2024-01-01')
        end_date = request.args.get('end_date', '2024-12-31')
        min_support = float(request.args.get('min_support', 0.01))
        limit = int(request.args.get('limit', 20))
        
        # Extract data
        orders = etl.extract_orders(start_date, end_date)
        order_items = etl._generate_sample_order_items(orders)
        
        # Calculate affinity
        affinity = etl.analyze_product_affinity(orders, order_items)
        
        # Filter by minimum support
        affinity = affinity[affinity['support'] >= min_support]
        
        # Get top pairs
        top_pairs = affinity.head(limit).to_dict(orient='records')
        
        return jsonify({
            'success': True,
            'product_pairs': top_pairs,
            'total_pairs': len(affinity),
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error calculating product affinity: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v1/cohorts/retention', methods=['GET'])
def get_cohort_retention():
    """
    Get cohort retention analysis
    
    Query params:
        start_date: Start date
        end_date: End date
    """
    try:
        start_date = request.args.get('start_date', '2024-01-01')
        end_date = request.args.get('end_date', '2024-12-31')
        
        # Extract orders and calculate cohorts
        orders = etl.extract_orders(start_date, end_date)
        cohorts = etl.analyze_cohorts(orders)
        
        # Convert to dict (handle Period index)
        cohorts_dict = {}
        for idx in cohorts.index:
            cohorts_dict[str(idx)] = cohorts.loc[idx].to_dict()
        
        return jsonify({
            'success': True,
            'cohort_retention': cohorts_dict,
            'num_cohorts': len(cohorts),
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error calculating cohort retention: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v1/customers/behavior', methods=['GET'])
def get_customer_behavior():
    """
    Get customer behavior metrics
    
    Query params:
        start_date: Start date
        end_date: End date
        customer_id: Specific customer ID (optional)
    """
    try:
        start_date = request.args.get('start_date', '2024-01-01')
        end_date = request.args.get('end_date', '2024-12-31')
        customer_id = request.args.get('customer_id')
        
        # Extract and transform
        events = etl.extract_web_events(start_date, end_date)
        behavior = etl.transform_customer_behavior(events)
        
        # Filter by customer if provided
        if customer_id:
            behavior = behavior[behavior['customer_id'] == customer_id]
        
        # Calculate summary metrics
        summary = {
            'total_sessions': len(behavior),
            'avg_session_duration': float(behavior['session_duration_seconds'].mean()),
            'conversion_rate': float(behavior['converted'].mean() * 100),
            'cart_abandonment_rate': float(behavior['abandoned_cart'].mean() * 100),
            'avg_page_views': float(behavior['page_views_sum'].mean())
        }
        
        # Get sample sessions
        sample_sessions = behavior.head(10).to_dict(orient='records')
        
        return jsonify({
            'success': True,
            'summary': summary,
            'sample_sessions': sample_sessions,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error getting customer behavior: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/v1/metrics/kpis', methods=['GET'])
def get_kpis():
    """Get key performance indicators"""
    try:
        start_date = request.args.get('start_date', '2024-01-01')
        end_date = request.args.get('end_date', '2024-12-31')
        
        # Run full pipeline
        results = etl.run_full_pipeline(start_date, end_date)
        
        # Calculate KPIs
        summary = results['summary']
        funnel = results['conversion_funnel']
        behavior = results['customer_behavior']
        
        kpis = {
            'revenue': {
                'total_revenue': round(summary['total_revenue'], 2),
                'avg_order_value': round(summary['avg_order_value'], 2),
                'revenue_per_customer': round(
                    summary['total_revenue'] / summary['unique_customers'], 2
                )
            },
            'conversion': {
                'overall_rate': round(float(
                    funnel[funnel['stage'] == 'purchase']['conversion_rate'].iloc[0]
                ), 2),
                'cart_abandonment_rate': round(float(
                    behavior['abandoned_cart'].mean() * 100
                ), 2)
            },
            'engagement': {
                'avg_session_duration': round(float(
                    behavior['session_duration_seconds'].mean()
                ), 2),
                'avg_page_views': round(float(
                    behavior['page_views_sum'].mean()
                ), 2)
            },
            'customers': {
                'total_customers': summary['unique_customers'],
                'total_orders': summary['total_orders'],
                'orders_per_customer': round(
                    summary['total_orders'] / summary['unique_customers'], 2
                )
            }
        }
        
        return jsonify({
            'success': True,
            'kpis': kpis,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error calculating KPIs: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
