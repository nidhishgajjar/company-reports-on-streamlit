import pandas as pd
from datetime import datetime, timedelta
import logging
from pathlib import Path
from typing import Tuple, Dict, List

class CustomerEngagementAnalyzer:
    def __init__(self, 
                 csv_path: str,
                 recent_window_months: int = 3,
                 historical_window_months: int = 9,
                 min_spend_threshold: float = 100.0):
        """
        Initialize the Customer Engagement Analyzer.
        
        Args:
            csv_path: Path to the customer transaction CSV file
            recent_window_months: Number of months for recent activity window
            historical_window_months: Number of months for historical baseline
            min_spend_threshold: Minimum spending amount to consider customer as active
        """
        self.csv_path = Path(csv_path)
        self.recent_window_months = recent_window_months
        self.historical_window_months = historical_window_months
        self.min_spend_threshold = min_spend_threshold
        self.logger = self._setup_logging()
        
        # Initialize dataframes
        self.df = None
        self.recent_customers = None
        self.historical_customers = None
        self.disengaged_customers = None

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the analyzer."""
        logger = logging.getLogger('CustomerEngagementAnalyzer')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger

    def load_data(self) -> None:
        """Load and preprocess the customer transaction data."""
        try:
            # First, read the entire file to find where CUSTOMER DETAILS section starts
            with open(self.csv_path, 'r') as f:
                lines = f.readlines()
            
            # Find the index of the CUSTOMER DETAILS line
            customer_details_index = next(
                (i for i, line in enumerate(lines) if 'CUSTOMER DETAILS' in line),
                -1
            )
            
            if customer_details_index == -1:
                raise ValueError("Could not find CUSTOMER DETAILS section in the CSV file")
            
            # Read the CSV starting from the line after CUSTOMER DETAILS
            self.df = pd.read_csv(
                self.csv_path,
                skiprows=customer_details_index + 1,
                dtype={
                    'rank': int,
                    'customer_id': str,
                    'email': str,
                    'name': str,
                    'total_spend': str,
                    'transaction_count': int,
                    'last_payment_amount': float,
                    'last_payment_date': str,
                    'last_payment_method': str
                }
            )
            
            # Convert date strings to datetime
            self.df['last_payment_date'] = pd.to_datetime(self.df['last_payment_date'])
            
            # Convert total_spend to numeric, removing currency symbols
            self.df['total_spend'] = self.df['total_spend'].str.replace('$', '').str.replace(',', '').astype(float)
            
            self.logger.info(f"Successfully loaded data from {self.csv_path}")
            self.logger.info(f"Total customers loaded: {len(self.df)}")
            
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise

    def analyze_engagement(self) -> Tuple[pd.DataFrame, Dict]:
        """
        Analyze customer engagement patterns with more sophisticated criteria.
        """
        current_date = datetime.now()
        recent_cutoff = current_date - timedelta(days=self.recent_window_months * 30)
        historical_cutoff = current_date - timedelta(days=(self.recent_window_months + self.historical_window_months) * 30)

        # Calculate engagement score based on multiple factors
        self.df['engagement_score'] = self.df.apply(
            lambda row: self._calculate_engagement_score(
                transaction_count=row['transaction_count'],
                total_spend=row['total_spend'],
                last_payment_date=row['last_payment_date'],
                current_date=current_date
            ),
            axis=1
        )

        # Engaged Customers: High engagement score OR recent activity with significant spend
        self.recent_customers = self.df[
            (
                (self.df['engagement_score'] >= 7) |  # High engagement score
                (
                    (self.df['last_payment_date'] >= recent_cutoff) &  # Recent activity
                    (self.df['total_spend'] >= self.min_spend_threshold * 2)  # Significant spend
                )
            )
        ]

        # Historical Customers: Previous engagement but not recent
        self.historical_customers = self.df[
            (self.df['last_payment_date'] < recent_cutoff) & 
            (self.df['last_payment_date'] >= historical_cutoff) &
            (self.df['total_spend'] >= self.min_spend_threshold)
        ]

        # Disengaged Customers: Declining engagement pattern
        self.disengaged_customers = self.historical_customers[
            ~self.historical_customers['customer_id'].isin(self.recent_customers['customer_id'])
        ]

        return self.disengaged_customers, self._calculate_metrics()

    def _calculate_engagement_score(self, 
                                  transaction_count: int,
                                  total_spend: float,
                                  last_payment_date: datetime,
                                  current_date: datetime) -> float:
        """
        Calculate an engagement score (0-10) based on multiple factors.
        """
        score = 0
        days_since_last_payment = (current_date - last_payment_date).days

        # Frequency score (0-3)
        if transaction_count >= 10:
            score += 3
        elif transaction_count >= 5:
            score += 2
        elif transaction_count >= 2:
            score += 1

        # Monetary score (0-4)
        if total_spend >= 5000:
            score += 4
        elif total_spend >= 1000:
            score += 3
        elif total_spend >= 500:
            score += 2
        elif total_spend >= self.min_spend_threshold:
            score += 1

        # Recency score (0-3)
        if days_since_last_payment <= 30:  # Last month
            score += 3
        elif days_since_last_payment <= 60:  # Last 2 months
            score += 2
        elif days_since_last_payment <= 90:  # Last 3 months
            score += 1

        return score

    def _calculate_metrics(self) -> Dict:
        """
        Calculate metrics for the analyzed engagement data.
        
        Returns:
            Dictionary containing metrics
        """
        metrics = {
            'total_customers': len(self.df),
            'active_recent_customers': len(self.recent_customers),
            'historical_only_customers': len(self.historical_customers),
            'disengaged_customers': len(self.disengaged_customers),
            'total_disengaged_value': self.disengaged_customers['total_spend'].sum()
        }

        self.logger.info(f"Analysis complete. Found {metrics['disengaged_customers']} disengaged customers")
        return metrics

    def generate_engagement_targets(self) -> pd.DataFrame:
        """
        Generate a detailed report of engagement targets with contact information.
        
        Returns:
            DataFrame containing engagement target details
        """
        if self.disengaged_customers is None:
            self.analyze_engagement()

        engagement_targets = self.disengaged_customers.copy()
        
        # Calculate days since last transaction
        engagement_targets['days_since_last_transaction'] = (
            datetime.now() - engagement_targets['last_payment_date']
        ).dt.days

        # Classify disengagement reasons
        engagement_targets['disengagement_reason'] = engagement_targets.apply(
            self._classify_disengagement_reason, axis=1
        )

        # Sort by total spend (highest value customers first)
        engagement_targets = engagement_targets.sort_values('total_spend', ascending=False)

        return engagement_targets[
            ['customer_id', 'name', 'email', 'total_spend', 'last_payment_date',
             'days_since_last_transaction', 'disengagement_reason']
        ]

    def _classify_disengagement_reason(self, row: pd.Series) -> str:
        """
        Classify the reason for customer disengagement based on their data.
        
        Args:
            row: Customer data row
            
        Returns:
            String describing the likely reason for disengagement
        """
        days_inactive = (datetime.now() - row['last_payment_date']).days
        
        if days_inactive > 270:  # 9 months
            return "Long-term inactive"
        elif row['transaction_count'] <= 2:
            return "Low engagement history"
        elif row['total_spend'] > 5000:
            return "High-value customer at risk"
        else:
            return "Standard churn risk"

    def export_results(self, output_dir: str = "reports") -> List[str]:
        """
        Export analysis results to CSV and TXT files for both engaged and disengaged customers.
        
        Args:
            output_dir: Directory to save the output files
            
        Returns:
            List of paths to the generated files
        """
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Get engagement data
        disengaged_customers, metrics = self.analyze_engagement()
        engaged_customers = self.recent_customers.copy()
        
        # Calculate engaged customers metrics
        engaged_metrics = {
            'total_customers': len(self.df),
            'active_customers': len(engaged_customers),
            'total_active_value': engaged_customers['total_spend'].sum(),
            'average_spend': engaged_customers['total_spend'].mean(),
            'total_transactions': engaged_customers['transaction_count'].sum()
        }
        
        # Generate summary text
        def generate_summary(title: str, stats: Dict) -> str:
            summary = f"SUMMARY STATISTICS - {title}\n"
            summary += "Metric,Value\n"
            summary += f"Report Period,Past {self.recent_window_months} months\n"
            summary += f"Generation Date,{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            for key, value in stats.items():
                if isinstance(value, float):
                    summary += f"{key.replace('_', ' ').title()},${value:,.2f}\n"
                else:
                    summary += f"{key.replace('_', ' ').title()},{value:,}\n"
            return summary
        
        generated_files = []
        
        # Export engaged customers
        if not engaged_customers.empty:
            # CSV file
            engaged_csv = output_path / f"engaged_customers_{timestamp}.csv"
            with open(engaged_csv, 'w', encoding='utf-8') as f:
                f.write(generate_summary("Engaged Customers", engaged_metrics))
                f.write("\nCUSTOMER DETAILS\n")
                engaged_customers.to_csv(f, index=False)
            generated_files.append(str(engaged_csv))
            
            # TXT file
            engaged_txt = output_path / f"engaged_customers_{timestamp}.txt"
            with open(engaged_txt, 'w', encoding='utf-8') as f:
                f.write("=== Engaged Customers Report ===\n\n")
                f.write(generate_summary("Engaged Customers", engaged_metrics).replace(',', ': '))
                f.write("\nCUSTOMER DETAILS\n")
                f.write("=" * 80 + "\n")
                for _, customer in engaged_customers.iterrows():
                    f.write(f"\nCustomer: {customer['name']}\n")
                    f.write(f"Email: {customer['email']}\n")
                    f.write(f"Total Spend: ${customer['total_spend']:,.2f}\n")
                    f.write(f"Transactions: {customer['transaction_count']}\n")
                    f.write(f"Last Payment: {customer['last_payment_date'].strftime('%Y-%m-%d')}\n")
                    f.write("-" * 80 + "\n")
            generated_files.append(str(engaged_txt))
        
        # Export disengaged customers
        if not disengaged_customers.empty:
            # CSV file
            disengaged_csv = output_path / f"disengaged_customers_{timestamp}.csv"
            with open(disengaged_csv, 'w', encoding='utf-8') as f:
                f.write(generate_summary("Disengaged Customers", metrics))
                f.write("\nCUSTOMER DETAILS\n")
                disengaged_customers.to_csv(f, index=False)
            generated_files.append(str(disengaged_csv))
            
            # TXT file
            disengaged_txt = output_path / f"disengaged_customers_{timestamp}.txt"
            with open(disengaged_txt, 'w', encoding='utf-8') as f:
                f.write("=== Disengaged Customers Report ===\n\n")
                f.write(generate_summary("Disengaged Customers", metrics).replace(',', ': '))
                f.write("\nCUSTOMER DETAILS\n")
                f.write("=" * 80 + "\n")
                for _, customer in disengaged_customers.iterrows():
                    f.write(f"\nCustomer: {customer['name']}\n")
                    f.write(f"Email: {customer['email']}\n")
                    f.write(f"Total Historical Spend: ${customer['total_spend']:,.2f}\n")
                    f.write(f"Days Since Last Transaction: {(datetime.now() - customer['last_payment_date']).days}\n")
                    f.write(f"Disengagement Reason: {self._classify_disengagement_reason(customer)}\n")
                    f.write("-" * 80 + "\n")
            generated_files.append(str(disengaged_txt))
        
        self.logger.info(f"Results exported to {output_dir}")
        return generated_files

def main():
    # Example usage
    analyzer = CustomerEngagementAnalyzer(
        csv_path="customer_insights_past_12_months_20241126_162024.csv",
        recent_window_months=3,
        historical_window_months=9,
        min_spend_threshold=100.0
    )
    
    analyzer.load_data()
    
    # Export results
    output_files = analyzer.export_results()
    
    print("\nAnalysis Complete!")
    print("Generated files:")
    for file in output_files:
        print(f"- {file}")

if __name__ == "__main__":
    main() 