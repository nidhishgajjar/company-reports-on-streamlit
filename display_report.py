import streamlit as st
import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def load_latest_report(reports_dir: str = "report_to_display"):
    """Load the most recent JSON report."""
    reports_path = Path(reports_dir)
    if not reports_path.exists() or not any(reports_path.iterdir()):
        return None
    json_files = list(reports_path.glob("customer_engagement_report_*.json"))
    if not json_files:
        return None
    latest_report = max(json_files, key=lambda x: x.stat().st_mtime)
    with open(latest_report, 'r', encoding='utf-8') as f:
        return json.load(f)

def create_gauge_chart(value, title, max_value=10):
    """Create a gauge chart for metrics."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        title={'text': title},
        gauge={
            'axis': {'range': [None, max_value]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, max_value/3], 'color': "lightgray"},
                {'range': [max_value/3, 2*max_value/3], 'color': "gray"},
                {'range': [2*max_value/3, max_value], 'color': "darkgray"}
            ]
        }
    ))
    fig.update_layout(height=200)
    return fig

def create_payment_timeline(df):
    """Create a timeline visualization of payments."""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['last_payment_date'],
        y=df['total_spend'],
        mode='markers',
        name='Payments',
        marker=dict(
            size=df['transaction_count']*5,
            color=df['engagement_score'],
            colorscale='Viridis',
            showscale=True
        ),
        text=df['name'],
        hovertemplate="<b>%{text}</b><br>" +
                      "Date: %{x}<br>" +
                      "Amount: $%{y:,.2f}<br>" +
                      "Engagement Score: %{marker.color:.1f}<br>"
    ))
    
    fig.update_layout(
        title="Payment Timeline",
        xaxis_title="Date",
        yaxis_title="Total Spend ($)",
        height=400
    )
    return fig

def main():
    st.set_page_config(page_title="Customer Engagement Report", layout="wide")
    
    # Custom CSS for Apple-like minimalist design
    st.markdown("""
        <style>
        /* Clean, minimal styling */
        .main {
            padding: 2rem;
            background-color: #ffffff;
        }
        
        /* Header styling */
        h1 {
            font-weight: 500;
            color: #1d1d1f;
            padding: 1.5rem 0;
            font-size: 2.5rem;
        }
        
        h2 {
            font-weight: 400;
            color: #1d1d1f;
            padding: 2rem 0 1rem 0;
            font-size: 1.8rem;
        }
        
        h3 {
            font-weight: 400;
            color: #1d1d1f;
            padding: 1.5rem 0 1rem 0;
            font-size: 1.5rem;
        }
        
        /* Metric card styling */
        .metric-card {
            background-color: #f5f5f7;
            border-radius: 1rem;
            padding: 1.5rem;
            margin: 0.5rem 0;
            box-shadow: 0 2px 6px rgba(0,0,0,0.05);
        }
        
        /* Plot styling */
        .stPlotlyChart {
            background-color: #ffffff;
            padding: 1.5rem;
            border-radius: 1rem;
            box-shadow: 0 2px 6px rgba(0,0,0,0.05);
            margin: 1.5rem 0;
        }
        
        /* Table styling */
        .dataframe {
            border: none !important;
            border-radius: 1rem;
            overflow: hidden;
        }
        
        /* Tabs styling */
        .stTabs [data-baseweb="tab-list"] {
            gap: 2rem;
            padding: 0.5rem 0;
        }
        
        .stTabs [data-baseweb="tab"] {
            padding: 1rem 2rem;
            background-color: #f5f5f7;
            border-radius: 0.5rem;
        }
        
        /* Info box styling */
        .stAlert {
            background-color: #f5f5f7;
            border: none;
            border-radius: 1rem;
            padding: 1rem;
        }
        
        /* Add spacing between sections */
        .section-spacing {
            margin: 2.5rem 0;
        }
        
        /* Sticky tabs styling */
        [data-testid="stVerticalBlock"] > [style*="flex-direction: column;"] > [data-testid="stVerticalBlock"] {
            position: sticky;
            top: 0;
            background-color: white;
            z-index: 999;
            padding: 1rem 0;
        }
        
        /* Reduce spacing between tabs and content */
        .stTabs [data-baseweb="tab-list"] {
            gap: 1rem;
            padding: 0;
            margin-bottom: 1rem;
            background-color: white;
        }
        
        .stTabs [data-baseweb="tab"] {
            padding: 0.75rem 1.5rem;
            background-color: #f5f5f7;
            border-radius: 0.5rem;
        }
        
        .stTabs [data-baseweb="tab-panel"] {
            padding-top: 1rem;
        }
        
        /* Reduce section spacing */
        .section-spacing {
            margin: 1.5rem 0;
        }
        
        /* Adjust segment description spacing */
        .segment-description {
            background-color: #f5f5f7;
            padding: 0.75rem 1rem;
            border-radius: 0.75rem;
            margin: 0.5rem 0 1rem 0;
        }
        
        /* Segment header styling */
        .segment-header {
            display: flex;
            align-items: center;
            margin-bottom: 1.5rem;
            padding-bottom: 0.5rem;
            border-bottom: 1px solid #f5f5f7;
        }
        
        .segment-title {
            font-size: 1.2rem;
            color: #1d1d1f;
            margin: 0;
            padding-right: 0.5rem;
        }
        
        .segment-description {
            font-size: 0.9rem;
            color: #86868b;
            font-weight: normal;
            margin: 0;
        }
        
        /* Sticky tabs with integrated descriptions */
        .stTabs [data-baseweb="tab-panel"] {
            padding-top: 0;
        }
        
        .stTabs [data-baseweb="tab"] {
            padding: 1rem 1.5rem;
            background-color: #f5f5f7;
            border-radius: 0.5rem;
            position: relative;
        }
        
        .stTabs [data-baseweb="tab-list"] {
            background-color: white;
            padding: 0.5rem 0;
            margin-bottom: 0;
        }
        </style>
    """, unsafe_allow_html=True)
    
    st.title("Customer Engagement Dashboard")
    
    report_data = load_latest_report()
    if report_data is None:
        st.error("No reports found in the reports directory.")
        return
    
    # Metadata in sidebar with minimal design
    with st.sidebar:
        st.subheader("Report Information")
        st.markdown(f"""
            <div style='color: #86868b; margin-top: 1rem;'>
                <p>{report_data['metadata']['report_period']}</p>
                <p>Generated: {report_data['metadata']['generation_date']}</p>
            </div>
        """, unsafe_allow_html=True)
    
    # Key Metrics with spacing
    st.markdown("<div class='section-spacing'></div>", unsafe_allow_html=True)
    st.header("Key Metrics")
    
    metrics = report_data['metrics']
    
    # Create metric cards with equal spacing
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
            <div class='metric-card'>
                <h4 style='color: #86868b; font-size: 0.9rem; margin-bottom: 0.5rem;'>TOTAL CUSTOMERS</h4>
                <p style='font-size: 1.8rem; font-weight: 500; margin: 0;'>{}</p>
            </div>
        """.format(metrics["Total Customers"]), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
            <div class='metric-card'>
                <h4 style='color: #86868b; font-size: 0.9rem; margin-bottom: 0.5rem;'>ACTIVE CUSTOMERS</h4>
                <p style='font-size: 1.8rem; font-weight: 500; margin: 0;'>{}</p>
            </div>
        """.format(metrics["Active Customers"]), unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
            <div class='metric-card'>
                <h4 style='color: #86868b; font-size: 0.9rem; margin-bottom: 0.5rem;'>TOTAL REVENUE</h4>
                <p style='font-size: 1.8rem; font-weight: 500; margin: 0;'>{}</p>
            </div>
        """.format(metrics["Total Revenue"]), unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
            <div class='metric-card'>
                <h4 style='color: #86868b; font-size: 0.9rem; margin-bottom: 0.5rem;'>AVG DAYS BETWEEN PAYMENTS</h4>
                <p style='font-size: 1.8rem; font-weight: 500; margin: 0;'>{} days</p>
            </div>
        """.format(metrics.get("Avg Days Between Payments", "N/A")), unsafe_allow_html=True)
    
    # Customer Segments Analysis
    st.markdown("<div class='section-spacing'></div>", unsafe_allow_html=True)
    st.header("Customer Segments")
    
    # Wrap tabs in a container
    segment_container = st.container()
    with segment_container:
        tabs = st.tabs([
            "Stable Customers",
            "Needs Attention",
            "Critical Follow-up"
        ])
        
        segments = [
            "Stable Customers",
            "Needs Attention",
            "Critical Follow-up"
        ]
        
        for tab, segment in zip(tabs, segments):
            with tab:
                segment_customers = report_data['segments'].get(segment, [])
                if segment_customers:
                    df = pd.DataFrame(segment_customers)
                    
                    # Segment Overview with added top margin
                    st.markdown("<div style='margin-top: 1.5rem;'></div>", unsafe_allow_html=True)
                    overview_cols = st.columns(3)
                    
                    with overview_cols[0]:
                        st.markdown("""
                            <div class='metric-card'>
                                <h4 style='color: #86868b; font-size: 0.9rem; margin-bottom: 0.5rem;'>CUSTOMERS</h4>
                                <p style='font-size: 1.8rem; font-weight: 500; margin: 0;'>{}</p>
                            </div>
                        """.format(len(df)), unsafe_allow_html=True)
                    
                    with overview_cols[1]:
                        st.markdown("""
                            <div class='metric-card'>
                                <h4 style='color: #86868b; font-size: 0.9rem; margin-bottom: 0.5rem;'>TOTAL REVENUE</h4>
                                <p style='font-size: 1.8rem; font-weight: 500; margin: 0;'>${:,.2f}</p>
                            </div>
                        """.format(df['total_spend'].sum()), unsafe_allow_html=True)
                    
                    with overview_cols[2]:
                        avg_frequency = df['payment_frequency_days'].mean()
                        st.markdown("""
                            <div class='metric-card'>
                                <h4 style='color: #86868b; font-size: 0.9rem; margin-bottom: 0.5rem;'>AVG PAYMENT FREQUENCY</h4>
                                <p style='font-size: 1.8rem; font-weight: 500; margin: 0;'>{:.2f} days</p>
                            </div>
                        """.format(avg_frequency if pd.notna(avg_frequency) else 0), unsafe_allow_html=True)
                    
                    # Customer Details Table
                    st.markdown("<div style='margin: 2rem 0;'></div>", unsafe_allow_html=True)
                    st.subheader("Customer Details")
                    display_columns = [
                        'name', 'email', 'total_spend', 'transaction_count',
                        'payment_frequency_days', 'days_since_last_payment',
                        'days_until_next_payment', 'payment_status'
                    ]
                    
                    if all(col in df.columns for col in display_columns):
                        display_df = df[display_columns]
                        st.dataframe(
                            display_df.style.format({
                                'total_spend': '${:,.2f}',
                                'payment_frequency_days': '{:.2f}',
                                'days_since_last_payment': '{:.0f}',
                                'days_until_next_payment': '{:.0f}'
                            }),
                            height=400
                        )
                    
                    # Payment Patterns Visualization
                    st.markdown("<div style='margin: 2rem 0;'></div>", unsafe_allow_html=True)
                    st.subheader("Payment Patterns")
                    
                    payment_pattern = px.scatter(
                        df,
                        x='days_since_last_payment',
                        y='payment_frequency_days',
                        size='total_spend',
                        hover_data=['name', 'email'],
                        title=f"Payment Pattern Analysis - {segment}",
                        labels={
                            'days_since_last_payment': 'Days Since Last Payment',
                            'payment_frequency_days': 'Payment Frequency (Days)',
                        }
                    )
                    
                    # Update plot styling
                    payment_pattern.update_layout(
                        plot_bgcolor='white',
                        paper_bgcolor='white',
                        font={'color': '#1d1d1f'},
                        title_font_size=20,
                        showlegend=False,
                        margin=dict(t=40, b=40, l=40, r=40)
                    )
                    
                    payment_pattern.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#f5f5f7')
                    payment_pattern.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#f5f5f7')
                    
                    st.plotly_chart(payment_pattern, use_container_width=True)
                else:
                    st.info(f"No customers in {segment}")

if __name__ == "__main__":
    main() 