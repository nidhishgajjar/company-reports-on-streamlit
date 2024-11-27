import stripe
from datetime import datetime, timedelta
import os
from typing import List, Dict
from collections import defaultdict
from dataclasses import dataclass, asdict
from decimal import Decimal
import pandas as pd

@dataclass
class CustomerInsight:
    customer_id: str
    email: str = ""
    name: str = ""
    total_spend: Decimal = Decimal('0')
    last_payment_amount: Decimal = Decimal('0')
    last_payment_date: datetime = None
    last_payment_method: str = ""
    transaction_count: int = 0

class StripeAnalytics:
    def __init__(self):
        self.api_key = os.environ.get('STRIPE_SECRET_KEY')
        if not self.api_key:
            raise ValueError("STRIPE_SECRET_KEY not found in environment variables")
        stripe.api_key = self.api_key

    def get_customer_details(self, customer_id: str) -> Dict:
        """Fetch detailed customer information from Stripe."""
        try:
            customer = stripe.Customer.retrieve(customer_id)
            return {
                'email': customer.email or "",
                'name': customer.name or "",
            }
        except stripe.error.StripeError:
            return {'email': "", 'name': ""}

    def get_payment_method_details(self, payment_method_id: str) -> str:
        """Fetch payment method details."""
        try:
            if not payment_method_id:
                return "Unknown"
            pm = stripe.PaymentMethod.retrieve(payment_method_id)
            return f"{pm.type} - {pm.card.brand} ending in {pm.card.last4}" if hasattr(pm, 'card') else pm.type
        except stripe.error.StripeError:
            return "Unknown"

    def get_customer_insights(self, months: int = 12) -> List[CustomerInsight]:
        """
        Analyze customer payment behavior for the specified period.
        """
        three_months_ago = datetime.now() - timedelta(days=30 * months)
        timestamp = int(three_months_ago.timestamp())

        # Dictionary to store customer insights
        customers = defaultdict(lambda: CustomerInsight(customer_id=""))
        
        try:
            # Fetch all payments after the timestamp
            payments = stripe.PaymentIntent.list(
                created={'gte': timestamp},
                limit=100,
                expand=['data.customer']
            )

            # Process payments and aggregate customer data
            for payment in payments.auto_paging_iter():
                if payment.status != 'succeeded':
                    continue

                customer_id = payment.customer.id if payment.customer else 'anonymous'
                payment_date = datetime.fromtimestamp(payment.created)
                payment_amount = Decimal(str(payment.amount / 100))

                if customer_id not in customers:
                    customer_details = self.get_customer_details(customer_id)
                    customers[customer_id] = CustomerInsight(
                        customer_id=customer_id,
                        email=customer_details['email'],
                        name=customer_details['name']
                    )

                customer = customers[customer_id]
                customer.total_spend += payment_amount
                customer.transaction_count += 1

                # Update last payment details if this is the most recent
                if not customer.last_payment_date or payment_date > customer.last_payment_date:
                    customer.last_payment_date = payment_date
                    customer.last_payment_amount = payment_amount
                    customer.last_payment_method = self.get_payment_method_details(payment.payment_method)

            # Convert to list and sort by total spend
            customer_list = list(customers.values())
            customer_list.sort(key=lambda x: x.total_spend, reverse=True)
            
            return customer_list

        except stripe.error.StripeError as e:
            print(f"An error occurred: {str(e)}")
            return []

    @staticmethod
    def generate_filename(base_name: str, months: int, extension: str) -> str:
        """Generate filename with timestamp and period."""
        current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{base_name}_past_{months}_months_{current_time}.{extension}"

    def save_customer_insights(self, insights: List[CustomerInsight], months: int = 3):
        """Save formatted customer insights to a text file."""
        if not insights:
            print("No data to save")
            return

        filename = self.generate_filename("customer_insights", months, "txt")

        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"=== Customer Payment Insights - Past {months} Months ===\n")
            f.write(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for rank, customer in enumerate(insights, 1):
                f.write(f"Rank #{rank} - Customer Summary\n")
                f.write(f"{'=' * 40}\n")
                f.write(f"Customer ID: {customer.customer_id}\n")
                if customer.email:
                    f.write(f"Email: {customer.email}\n")
                if customer.name:
                    f.write(f"Name: {customer.name}\n")
                f.write(f"Total Spend: ${customer.total_spend:,.2f}\n")
                f.write(f"Transaction Count: {customer.transaction_count}\n")
                f.write(f"Last Payment: ${customer.last_payment_amount:,.2f}\n")
                f.write(f"Last Payment Date: {customer.last_payment_date.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Last Payment Method: {customer.last_payment_method}\n")
                f.write(f"{'-' * 40}\n\n")
        
        print(f"\nCustomer insights saved to {filename}")

    def export_to_csv(self, insights: List[CustomerInsight], months: int = 3):
        """Export customer insights to a CSV file using pandas."""
        if not insights:
            print("No data to export")
            return

        filename = self.generate_filename("customer_insights", months, "csv")

        # Convert insights to list of dictionaries
        data = []
        for rank, customer in enumerate(insights, 1):
            customer_dict = asdict(customer)
            if customer_dict['last_payment_date']:
                customer_dict['last_payment_date'] = customer_dict['last_payment_date'].strftime('%Y-%m-%d %H:%M:%S')
            
            customer_dict['rank'] = rank
            customer_dict['total_spend'] = float(customer_dict['total_spend'])
            customer_dict['last_payment_amount'] = float(customer_dict['last_payment_amount'])
            
            data.append(customer_dict)

        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Calculate summary statistics
        total_customers = len(df)
        total_revenue = df['total_spend'].sum()
        avg_spend = df['total_spend'].mean()
        total_transactions = df['transaction_count'].sum()
        
        # Create summary statistics DataFrame
        summary_stats = pd.DataFrame([
            ['Report Period', f"Past {months} months"],
            ['Generation Date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
            ['Total Customers', total_customers],
            ['Total Revenue', f"${total_revenue:,.2f}"],
            ['Average Spend per Customer', f"${avg_spend:,.2f}"],
            ['Total Transactions', total_transactions]
        ], columns=['Metric', 'Value'])
        
        # Reorder columns for better readability
        columns_order = [
            'rank',
            'customer_id',
            'email',
            'name',
            'total_spend',
            'transaction_count',
            'last_payment_amount',
            'last_payment_date',
            'last_payment_method'
        ]
        
        df = df[columns_order]
        
        # Export summary stats and data to CSV
        with open(filename, 'w', encoding='utf-8', newline='') as f:
            f.write("SUMMARY STATISTICS\n")
            summary_stats.to_csv(f, index=False)
            f.write('\nCUSTOMER DETAILS\n')
            df.to_csv(f, index=False)

        print(f"\nData exported to {filename}")
        
        # Print summary statistics to console
        print("\nSummary Statistics:")
        print(f"Total Customers: {total_customers}")
        print(f"Total Revenue: ${total_revenue:,.2f}")
        print(f"Average Spend per Customer: ${avg_spend:,.2f}")
        print(f"Total Transactions: {total_transactions}")

def main():
    analytics = StripeAnalytics()
    months = 12  # You can change this value as needed
    customer_insights = analytics.get_customer_insights(months=months)
    
    # Save insights to text file
    analytics.save_customer_insights(customer_insights, months=months)
    
    # Export to CSV
    analytics.export_to_csv(customer_insights, months=months)

if __name__ == "__main__":
    main() 