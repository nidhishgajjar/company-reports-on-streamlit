import stripe
from datetime import datetime, timedelta
import os
from typing import List, Dict, Tuple
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from decimal import Decimal
import pandas as pd
from pathlib import Path
import logging
import json
import statistics

@dataclass
class CustomerEngagement:
    customer_id: str
    email: str = ""
    name: str = ""
    total_spend: Decimal = Decimal('0')
    transaction_count: int = 0
    last_payment_amount: Decimal = Decimal('0')
    last_payment_date: datetime = None
    last_payment_method: str = ""
    payment_history: List[Dict] = field(default_factory=list)
    avg_payment_amount: Decimal = Decimal('0')
    payment_frequency_days: float = 0
    spending_trend: str = "stable"
    engagement_status: str = "active"
    engagement_score: float = 0.0
    days_since_last_payment: int = 0
    risk_level: str = "low"
    predicted_next_payment: datetime = None
    payment_status: str = "on_track"  # on_track, overdue, at_risk
    historical_engagement: str = "new"  # new, consistent, declining, dormant
    days_until_next_payment: int = 0
    payment_regularity_score: float = 0.0  # Score indicating payment pattern consistency

class CustomerEngagementReport:
    def __init__(self, months: int = 3):
        self.api_key = os.environ.get('STRIPE_SECRET_KEY')
        if not self.api_key:
            raise ValueError("STRIPE_SECRET_KEY not found in environment variables")
        stripe.api_key = self.api_key
        self.months = months
        self.logger = self._setup_logging()
        self.customers = []

    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger('CustomerEngagementReport')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    def fetch_stripe_data(self) -> None:
        """Fetch and process customer payment data from Stripe."""
        cutoff_date = datetime.now() - timedelta(days=30 * self.months)
        timestamp = int(cutoff_date.timestamp())
        
        customer_data = defaultdict(lambda: {
            'customer_id': '',
            'email': '',
            'name': '',
            'payments': [],
        })

        try:
            payments = stripe.PaymentIntent.list(
                created={'gte': timestamp},
                limit=100,
                expand=['data.customer', 'data.payment_method']
            )

            for payment in payments.auto_paging_iter():
                if payment.status != 'succeeded':
                    continue

                self._process_payment(payment, customer_data)

            self.customers = [
                self._analyze_customer_engagement(cust_id, data)
                for cust_id, data in customer_data.items()
            ]
            
            self.customers.sort(key=lambda x: x.total_spend, reverse=True)
            
        except stripe.error.StripeError as e:
            self.logger.error(f"Stripe API error: {str(e)}")
            raise

    def _process_payment(self, payment: stripe.PaymentIntent, customer_data: Dict) -> None:
        """Process individual payment and update customer data."""
        customer_id = payment.customer.id if payment.customer else 'anonymous'
        payment_date = datetime.fromtimestamp(payment.created)
        payment_amount = Decimal(str(payment.amount / 100))

        if customer_data[customer_id]['customer_id'] == '':
            customer_details = self._get_customer_details(customer_id)
            customer_data[customer_id].update({
                'customer_id': customer_id,
                'email': customer_details['email'],
                'name': customer_details['name']
            })

        customer_data[customer_id]['payments'].append({
            'date': payment_date,
            'amount': payment_amount,
            'method': self._get_payment_method_details(payment.payment_method)
        })

    def _analyze_customer_engagement(self, customer_id: str, data: Dict) -> CustomerEngagement:
        """Analyze customer engagement and predict next payment."""
        payments = sorted(data['payments'], key=lambda x: x['date'])
        if not payments:  # Handle case with no payments
            return CustomerEngagement(
                customer_id=customer_id,
                email=data['email'],
                name=data['name']
            )

        total_spend = sum(p['amount'] for p in payments)
        avg_amount = total_spend / len(payments) if payments else Decimal('0')
        
        last_payment = payments[-1]
        days_since_last = (datetime.now() - last_payment['date']).days
        
        # Calculate payment frequency and predict next payment
        payment_frequency = self._calculate_payment_frequency(payments)
        payment_status, predicted_next_payment, days_until_next = self._predict_next_payment(
            last_payment['date'], 
            payment_frequency, 
            days_since_last
        )
        
        # Analyze historical engagement
        historical_engagement = self._analyze_historical_engagement(
            payments,
            payment_frequency,
            days_since_last
        )
        
        # Calculate payment regularity score
        payment_regularity_score = self._calculate_payment_regularity(payments, payment_frequency)
        
        # Calculate spending trend
        spending_trend = self._calculate_spending_trend(payments)
        
        # Calculate engagement score
        engagement_score = self._calculate_engagement_score(
            transaction_count=len(payments),
            total_spend=total_spend,
            days_since_last=days_since_last,
            payment_frequency=payment_frequency
        )
        
        engagement_status = self._determine_engagement_status(engagement_score, days_since_last, historical_engagement)
        risk_level = self._assess_risk_level(engagement_score, total_spend, days_since_last)

        return CustomerEngagement(
            customer_id=customer_id,
            email=data['email'],
            name=data['name'],
            total_spend=total_spend,
            transaction_count=len(payments),
            last_payment_amount=last_payment['amount'],
            last_payment_date=last_payment['date'],
            last_payment_method=last_payment['method'],
            payment_history=payments,
            avg_payment_amount=avg_amount,
            payment_frequency_days=payment_frequency,
            spending_trend=spending_trend,
            engagement_status=engagement_status,
            engagement_score=engagement_score,
            days_since_last_payment=days_since_last,
            risk_level=risk_level,
            predicted_next_payment=predicted_next_payment,
            payment_status=payment_status,
            historical_engagement=historical_engagement,
            days_until_next_payment=days_until_next,
            payment_regularity_score=payment_regularity_score
        )

    def _calculate_payment_frequency(self, payments: List[Dict]) -> float:
        """Calculate average time between payments for a customer."""
        if len(payments) > 1:
            time_diffs = [(payments[i+1]['date'] - payments[i]['date']).days 
                         for i in range(len(payments)-1)]
            return sum(time_diffs) / len(time_diffs)
        return float('nan')  # Return NaN if there are fewer than 2 payments

    def _calculate_overall_avg_payment_frequency(self) -> float:
        """Calculate overall average payment frequency for all payments."""
        all_payment_dates = []

        for customer in self.customers:
            all_payment_dates.extend([p['date'] for p in customer.payment_history])

        # Debugging: Print all payment dates
        print(f"All payment dates: {all_payment_dates}")

        if len(all_payment_dates) > 1:
            all_payment_dates.sort()
            total_days = (all_payment_dates[-1] - all_payment_dates[0]).days
            total_intervals = len(all_payment_dates) - 1
            
            # Debugging: Print total days and intervals
            print(f"Total days: {total_days}, Total intervals: {total_intervals}")
            
            return total_days / total_intervals

        return float('nan')  # Return NaN if there are fewer than 2 payments

    def generate_report(self) -> str:
        """Generate JSON report with engagement analysis."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_dir = Path("report_to_display")
        report_dir.mkdir(exist_ok=True)
        
        overall_avg_payment_frequency = self._calculate_overall_avg_payment_frequency()

        report_data = {
            'metadata': {
                'timestamp': timestamp,
                'report_period': f"Past {self.months} months",
                'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            },
            'metrics': {
                'total_customers': len(self.customers),
                'active_customers': len([c for c in self.customers if c.engagement_status == "active"]),
                'total_revenue': float(sum(c.total_spend for c in self.customers)),
                'avg_payment_frequency': overall_avg_payment_frequency
            },
            'customers': [self._customer_to_dict(c) for c in self.customers]
        }
        
        report_file = report_dir / f"customer_engagement_report_{timestamp}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        return str(report_file)

    def _customer_to_dict(self, customer: CustomerEngagement) -> Dict:
        """Convert CustomerEngagement object to dictionary."""
        return {
            'name': customer.name,
            'email': customer.email,
            'total_spend': float(customer.total_spend),
            'transaction_count': customer.transaction_count,
            'avg_payment_amount': float(customer.avg_payment_amount),
            'payment_frequency_days': customer.payment_frequency_days,
            'spending_trend': customer.spending_trend,
            'engagement_status': customer.engagement_status,
            'days_since_last_payment': customer.days_since_last_payment,
            'last_payment_date': customer.last_payment_date.strftime('%Y-%m-%d') if customer.last_payment_date else None,
            'last_payment_amount': float(customer.last_payment_amount),
            'engagement_score': customer.engagement_score,
            'payment_status': customer.payment_status,
            'historical_engagement': customer.historical_engagement,
            'days_until_next_payment': customer.days_until_next_payment,
            'payment_regularity_score': customer.payment_regularity_score,
            'predicted_next_payment': customer.predicted_next_payment.strftime('%Y-%m-%d') if customer.predicted_next_payment else None,
            'risk_level': customer.risk_level
        }

    # Helper methods (implementation details omitted for brevity)
    def _get_customer_details(self, customer_id: str) -> Dict:
        """Fetch customer details from Stripe."""
        try:
            customer = stripe.Customer.retrieve(customer_id)
            return {
                'email': customer.email or "",
                'name': customer.name or "",
            }
        except stripe.error.StripeError:
            return {'email': "", 'name': ""}

    def _get_payment_method_details(self, payment_method_id: str) -> str:
        """Fetch payment method details."""
        try:
            if not payment_method_id:
                return "Unknown"
            pm = stripe.PaymentMethod.retrieve(payment_method_id)
            return f"{pm.type} - {pm.card.brand} ending in {pm.card.last4}" if hasattr(pm, 'card') else pm.type
        except stripe.error.StripeError:
            return "Unknown"

    def _calculate_spending_trend(self, payments: List[Dict]) -> str:
        """Calculate customer spending trend."""
        if len(payments) < 3:
            return "insufficient_data"
            
        # Sort payments by date
        sorted_payments = sorted(payments, key=lambda x: x['date'])
        
        # Calculate average spend for recent vs older payments
        recent_payments = sorted_payments[-3:]
        older_payments = sorted_payments[:-3]
        
        recent_avg = float(sum(p['amount'] for p in recent_payments)) / len(recent_payments)
        older_avg = float(sum(p['amount'] for p in older_payments)) / len(older_payments) if older_payments else recent_avg
        
        # Determine trend
        if recent_avg > older_avg * 1.2:  # 20% increase
            return "increasing"
        elif recent_avg < older_avg * 0.8:  # 20% decrease
            return "decreasing"
        return "stable"

    def _calculate_engagement_score(self, transaction_count: int, total_spend: Decimal, days_since_last: int, payment_frequency: float) -> float:
        """Calculate customer engagement score (0-10)."""
        score = 0
        # Frequency score (0-3)
        if transaction_count >= 10:
            score += 3
        elif transaction_count >= 5:
            score += 2
        elif transaction_count >= 2:
            score += 1

        # Monetary score (0-3)
        if total_spend >= 5000:
            score += 3
        elif total_spend >= 1000:
            score += 2
        elif total_spend >= 500:
            score += 1

        # Recency score (0-2)
        if days_since_last <= 30:
            score += 2
        elif days_since_last <= 90:
            score += 1

        # Payment regularity score (0-2)
        if payment_frequency > 0:
            if payment_frequency <= 45:
                score += 2
            elif payment_frequency <= 90:
                score += 1

        return score

    def _determine_engagement_status(self, score: float, days_since_last: int, historical_engagement: str) -> str:
        """Determine customer engagement status based on score, recency, and history."""
        if historical_engagement == "dormant" and score >= 5:
            return "potential_reengagement"
        elif score >= 7:
            return "active"
        elif score >= 4 and days_since_last <= 180:
            return "active"
        elif historical_engagement in ["declining", "dormant"]:
            return "disengaged"
        return "inactive"

    def _calculate_potential_reengagement_score(self, transaction_count: int, days_since_last: int) -> float:
        """Calculate potential re-engagement score based on transaction history and recency."""
        score = 0
        if transaction_count >= 2:
            score += 1
        if days_since_last <= 90:
            score += 2
        elif days_since_last <= 180:
            score += 1
        return score

    def _assess_risk_level(self, score: float, total_spend: Decimal, days_since_last: int) -> str:
        """Assess customer risk level based on engagement and value."""
        total_spend_float = float(total_spend)
        
        if score >= 7:
            return "low"
        
        if total_spend_float >= 5000:
            if days_since_last > 90:
                return "high"
            return "medium"
            
        if days_since_last > 180:
            return "high"
        elif days_since_last > 90:
            return "medium"
            
        return "low"

    def _calculate_overall_metrics(self) -> Dict:
        """Calculate overall engagement metrics."""
        total_customers = len(self.customers)
        if total_customers == 0:
            return {
                "Total Customers": 0,
                "Active Customers": 0,
                "Total Revenue": "$0.00",
                "Active Customer Percentage": "0.00%",
                "Avg Days Between Payments": 0.00
            }

        # Calculate active customers and percentage
        active_customers = len([c for c in self.customers if c.engagement_status == "active"])
        active_percentage = round((active_customers / total_customers) * 100, 2)
        
        total_spend = sum(c.total_spend for c in self.customers)
        
        # Calculate average payment frequency
        valid_frequencies = [
            c.payment_frequency_days 
            for c in self.customers 
            if not pd.isna(c.payment_frequency_days) and c.transaction_count > 1
        ]
        
        avg_payment_frequency = round(
            sum(valid_frequencies) / len(valid_frequencies)
            if valid_frequencies else 0.00,
            2
        )
        
        return {
            "Total Customers": total_customers,
            "Active Customers": active_customers,
            "Total Revenue": f"${total_spend:,.2f}",
            "Active Customer Percentage": f"{active_percentage:.2f}%",
            "Avg Days Between Payments": avg_payment_frequency
        }

    def _calculate_risk_segments(self) -> Dict:
        """Calculate customer segments based on risk levels with actionable names."""
        segments = {
            "Stable Customers": [],  # Was "Low Risk"
            "Needs Attention": [],   # Was "Medium Risk"
            "Critical Follow-up": [] # Was "High Risk"
        }
        
        for customer in self.customers:
            if customer.risk_level == "low":
                segments["Stable Customers"].append(self._customer_to_dict(customer))
            elif customer.risk_level == "medium":
                segments["Needs Attention"].append(self._customer_to_dict(customer))
            else:  # high risk
                segments["Critical Follow-up"].append(self._customer_to_dict(customer))
        
        return segments

    def manage_reports(self, new_report_files: List[str]) -> None:
        """
        Manage report files by maintaining one current report and archiving old ones.
        
        Args:
            new_report_files: List of paths to newly generated report files
        """
        # Create necessary directories if they don't exist
        report_display_dir = Path("report_to_display")
        all_reports_dir = Path("all_reports")
        
        report_display_dir.mkdir(exist_ok=True)
        all_reports_dir.mkdir(exist_ok=True)
        
        # Move existing reports to archive if they exist
        if any(report_display_dir.iterdir()):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_dir = all_reports_dir / f"report_{timestamp}"
            archive_dir.mkdir(exist_ok=True)
            
            for file in report_display_dir.iterdir():
                if file.is_file():
                    file.rename(archive_dir / file.name)
        
        # Copy new reports to display directory
        for file_path in new_report_files:
            file = Path(file_path)
            if file.exists():
                file.rename(report_display_dir / file.name)

    def export_results(self, output_dir: str = "reports") -> List[str]:
        """Generate and manage report data in JSON format."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        report_display_dir = Path("report_to_display")
        all_reports_dir = Path("all_reports")
        
        report_display_dir.mkdir(exist_ok=True)
        all_reports_dir.mkdir(exist_ok=True)
        
        # Archive existing reports
        if any(report_display_dir.iterdir()):
            archive_dir = all_reports_dir / f"report_{timestamp}"
            archive_dir.mkdir(exist_ok=True)
            for file in report_display_dir.iterdir():
                if file.is_file():
                    file.rename(archive_dir / file.name)
        
        # Generate report data with new segmentation
        report_data = {
            'metadata': {
                'timestamp': timestamp,
                'report_period': f"Past {self.months} months",
                'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            },
            'metrics': self._calculate_overall_metrics(),
            'segments': self._calculate_risk_segments()
        }
        
        report_file = report_display_dir / f"customer_engagement_report_{timestamp}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        return [str(report_file)]

    def _predict_next_payment(
        self, 
        last_payment_date: datetime, 
        payment_frequency: float,
        days_since_last: int
    ) -> Tuple[str, datetime, int]:
        """Predict next payment date and determine payment status."""
        if pd.isna(payment_frequency) or not last_payment_date:
            return "unknown", None, 0
        
        predicted_date = last_payment_date + timedelta(days=payment_frequency)
        days_until_next = (predicted_date - datetime.now()).days
        
        # Determine payment status
        if days_since_last > payment_frequency * 2:
            status = "at_risk"
        elif days_since_last > payment_frequency * 1.5:
            status = "overdue"
        else:
            status = "on_track"
        
        return status, predicted_date, days_until_next

    def _analyze_historical_engagement(
        self,
        payments: List[Dict],
        payment_frequency: float,
        days_since_last: int
    ) -> str:
        """Analyze historical engagement pattern."""
        if len(payments) < 2:
            return "new"
        
        # Calculate average frequency for first and second half of payments
        mid_point = len(payments) // 2
        first_half = payments[:mid_point]
        second_half = payments[mid_point:]
        
        first_half_freq = self._calculate_payment_frequency(first_half)
        second_half_freq = self._calculate_payment_frequency(second_half)
        
        if days_since_last > payment_frequency * 3:
            return "dormant"
        elif not pd.isna(first_half_freq) and not pd.isna(second_half_freq):
            if second_half_freq > first_half_freq * 1.5:
                return "declining"
            elif abs(second_half_freq - first_half_freq) <= first_half_freq * 0.2:
                return "consistent"
        
        return "irregular"

    def _calculate_payment_regularity(self, payments: List[Dict], avg_frequency: float) -> float:
        """Calculate how regular the payment pattern is (0-1 score)."""
        if len(payments) < 3 or pd.isna(avg_frequency) or avg_frequency == 0:
            return 0.0
        
        time_diffs = [(payments[i+1]['date'] - payments[i]['date']).days 
                      for i in range(len(payments)-1)]
        
        # Calculate standard deviation of payment intervals
        try:
            std_dev = statistics.stdev(time_diffs) if len(time_diffs) > 1 else float('inf')
            # Avoid division by zero
            if avg_frequency == 0:
                return 0.0
            regularity = 1 / (1 + (std_dev / avg_frequency))
            return min(1.0, regularity)
        except (statistics.StatisticsError, ZeroDivisionError):
            return 0.0

def main():
    try:
        report = CustomerEngagementReport(months=12)
        report.fetch_stripe_data()
        
        # Generate and manage reports
        generated_files = report.export_results()
        
        print("\nReport Generation Complete!")
        print("\nGenerated files:")
        for file in generated_files:
            print(f"- {file}")
        
        print("\nReports are available in:")
        print("- report_to_display/ (current report)")
        print("- all_reports/ (archived reports)")
        
    except Exception as e:
        print(f"Error generating report: {str(e)}")

if __name__ == "__main__":
    main() 