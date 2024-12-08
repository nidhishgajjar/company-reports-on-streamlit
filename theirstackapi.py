import os
import requests
from datetime import datetime, timedelta
import json

class TheirStackAPI:
    def __init__(self):
        self.api_key = os.environ.get('THEIRSTACK_API_KEY')
        self.base_url = 'https://api.theirstack.com/v1'
        
        if not self.api_key:
            raise ValueError("THEIRSTACK_API_KEY not found in environment variables")

    def search_jobs(self):
        endpoint = f"{self.base_url}/jobs/search"
        
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        payload = {
            "include_total_results": False,
            "order_by": [
                {
                    "desc": True,
                    "field": "date_posted"
                }
            ],
            "posted_at_max_age_days": 1,
            "job_country_code_or": ["CA"],
            "page": 0,
            "limit": 10,
            "blur_company_data": True
        }
        
        try:
            response = requests.post(endpoint, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching jobs: {e}")
            return None

    def parse_and_save_jobs(self, jobs_data, output_file="jobs_output.txt"):
        """Parse jobs data and save to a text file"""
        if not jobs_data or 'data' not in jobs_data:
            print("No jobs data to parse")
            return

        with open(output_file, 'w', encoding='utf-8') as f:
            # Write metadata
            metadata = jobs_data.get('metadata', {})
            f.write("=== SEARCH METADATA ===\n")
            f.write(f"Total Results: {metadata.get('total_results', 'N/A')}\n")
            f.write(f"Total Companies: {metadata.get('total_companies', 'N/A')}\n")
            f.write(f"Truncated Results: {metadata.get('truncated_results', 'N/A')}\n")
            f.write(f"Truncated Companies: {metadata.get('truncated_companies', 'N/A')}\n")
            f.write("=" * 80 + "\n\n")

            for job in jobs_data['data']:
                company_info = job.get('company_object', {})
                
                # Format job details
                job_details = [
                    f"Job ID: {job.get('id', 'N/A')}",
                    f"Job Title: {job.get('job_title', 'N/A')}",
                    f"Normalized Title: {job.get('normalized_title', 'N/A')}",
                    f"Seniority: {job.get('seniority', 'N/A')}",
                    f"Has Blurred Data: {job.get('has_blurred_data', 'N/A')}",
                    
                    "\n=== Company Information ===",
                    f"Company: {company_info.get('name', 'N/A')}",
                    f"Company Domain: {job.get('company_domain', 'N/A')}",
                    f"Industry: {company_info.get('industry', 'N/A')}",
                    f"Industry ID: {company_info.get('industry_id', 'N/A')}",
                    f"Company Size: {company_info.get('employee_count_range', 'N/A')}",
                    f"Exact Employee Count: {company_info.get('employee_count', 'N/A')}",
                    f"Founded Year: {company_info.get('founded_year', 'N/A')}",
                    f"Company Logo: {company_info.get('logo', 'N/A')}",
                    f"LinkedIn URL: {company_info.get('linkedin_url', 'N/A')}",
                    f"LinkedIn ID: {company_info.get('linkedin_id', 'N/A')}",
                    f"Apollo ID: {company_info.get('apollo_id', 'N/A')}",
                    f"Is Recruiting Agency: {company_info.get('is_recruiting_agency', 'N/A')}",
                    f"Company Description: {company_info.get('long_description', 'N/A')}",
                    f"SEO Description: {company_info.get('seo_description', 'N/A')}",
                    
                    "\n=== Company Metrics ===",
                    f"Annual Revenue: {company_info.get('annual_revenue_usd_readable', 'N/A')}",
                    f"Total Funding: {company_info.get('total_funding_usd', 'N/A')}",
                    f"Last Funding Date: {company_info.get('last_funding_round_date', 'N/A')}",
                    f"Last Funding Amount: {company_info.get('last_funding_round_amount_readable', 'N/A')}",
                    f"Funding Stage: {company_info.get('funding_stage', 'N/A')}",
                    f"Number of Jobs: {company_info.get('num_jobs', 'N/A')}",
                    f"Jobs Last 30 Days: {company_info.get('num_jobs_last_30_days', 'N/A')}",
                    f"Alexa Ranking: {company_info.get('alexa_ranking', 'N/A')}",
                    f"Stock Symbol: {company_info.get('publicly_traded_symbol', 'N/A')}",
                    f"Stock Exchange: {company_info.get('publicly_traded_exchange', 'N/A')}",
                    
                    "\n=== Location Details ===",
                    f"Location: {job.get('location', 'N/A')}",
                    f"Short Location: {job.get('short_location', 'N/A')}",
                    f"Long Location: {job.get('long_location', 'N/A')}",
                    f"Country: {job.get('country', 'N/A')}",
                    f"Countries: {', '.join(job.get('countries', []))}",
                    f"Country Code: {job.get('country_code', 'N/A')}",
                    f"Country Codes: {', '.join(job.get('country_codes', []))}",
                    f"State Code: {job.get('state_code', 'N/A')}",
                    f"Cities: {', '.join(job.get('cities', []))}",
                    f"Continents: {', '.join(job.get('continents', []))}",
                    f"Postal Code: {job.get('postal_code', 'N/A')}",
                    f"Latitude: {job.get('latitude', 'N/A')}",
                    f"Longitude: {job.get('longitude', 'N/A')}",
                    
                    "\n=== Work Type ===",
                    f"Remote: {job.get('remote', False)}",
                    f"Hybrid: {job.get('hybrid', False)}",
                    f"Employment Statuses: {', '.join(job.get('employment_statuses', []))}",
                    
                    "\n=== Salary Information ===",
                    f"Salary Range: {job.get('salary_string', 'N/A')}",
                    f"Min Annual Salary: {job.get('min_annual_salary', 'N/A')}",
                    f"Max Annual Salary: {job.get('max_annual_salary', 'N/A')}",
                    f"Min Annual Salary (USD): {job.get('min_annual_salary_usd', 'N/A')}",
                    f"Max Annual Salary (USD): {job.get('max_annual_salary_usd', 'N/A')}",
                    f"Average Annual Salary (USD): {job.get('avg_annual_salary_usd', 'N/A')}",
                    f"Currency: {job.get('salary_currency', 'N/A')}",
                    
                    "\n=== Technologies ===",
                    f"Technology Names: {', '.join(company_info.get('technology_names', []))}",
                    f"Technology Slugs: {', '.join(company_info.get('technology_slugs', []))}",
                    
                    "\n=== Hiring Team Contacts ===",
                ]
                
                # Add hiring team information
                hiring_team = job.get('hiring_team', [])
                if hiring_team:
                    for member in hiring_team:
                        member_details = [
                            f"\nTeam Member:",
                            f"Name: {member.get('full_name', 'N/A')}",
                            f"Role: {member.get('role', 'N/A')}",
                            f"LinkedIn: {member.get('linkedin_url', 'N/A')}"
                        ]
                        job_details.extend(member_details)
                else:
                    job_details.append("No hiring team information available")

                job_details.extend([
                    "\n=== URLs & Dates ===",
                    f"Job URL: {job.get('url', 'N/A')}",
                    f"Final URL: {job.get('final_url', 'N/A')}",
                    f"Source URL: {job.get('source_url', 'N/A')}",
                    f"Date Posted: {job.get('date_posted', 'N/A')}",
                    f"Date Discovered: {job.get('discovered_at', 'N/A')}",
                    f"Reposted: {job.get('reposted', False)}",
                    f"Date Reposted: {job.get('date_reposted', 'N/A')}",
                    
                    "\n=== Description ===",
                    f"{job.get('description', 'N/A')}\n",
                    
                    "=" * 80 + "\n"
                ])
                
                f.write("\n".join(job_details))

def main():
    api = TheirStackAPI()
    jobs = api.search_jobs()
    
    if jobs:
        api.parse_and_save_jobs(jobs)
        print("Jobs data has been saved to jobs_output.txt")
    else:
        print("Failed to fetch jobs data")

if __name__ == "__main__":
    main()
