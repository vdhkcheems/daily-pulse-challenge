#!/usr/bin/env python3
"""
Casting Pulse Builder - Generate daily aggregated casting data summaries.

This script processes raw casting breakdown data and generates a daily pulse report
with aggregated metrics by date, region, and project type.

Usage:
    python build_pulse.py --input breakdowns_sample.csv --output daily_pulse.csv

Requirements:
    - pandas
    - textblob
    - numpy
    - argparse
"""

import pandas as pd
import numpy as np
from textblob import TextBlob
import argparse
import re
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CastingPulseBuilder:
    def __init__(self):
        # Region mapping based on common casting locations
        self.region_map = {
            'los angeles': 'LA',
            'la': 'LA',
            'hollywood': 'LA',
            'burbank': 'LA',
            'santa monica': 'LA',
            'culver city': 'LA',
            'north hollywood': 'LA',
            'santa clarita': 'LA',
            'glendale': 'LA',
            'simi valley': 'LA',
            'fillmore': 'LA',
            'agoura hills': 'LA',
            'san juan capistrano': 'LA',
            'calabasas': 'LA',
            'altadena': 'LA',
            'venice': 'LA',
            'fullerton': 'LA',
            'la/oc': 'LA',
            'manhattan beach': 'LA',
            'santa ana': 'LA',
            'upland': 'LA',
            'camarillo': 'LA',
            'ontario': 'LA',
            'orange county': 'LA',
            'southern california': 'LA',
            'greater los angeles region': 'LA',
            'new york': 'NY',
            'nyc': 'NY',
            'manhattan': 'NY',
            'brooklyn': 'NY',
            'queens': 'NY',
            'white plains': 'NY',
            'long island': 'NY',
            'westchester': 'NY',
            'yonkers': 'NY',
            'astoria': 'NY',
            'south bronx': 'NY',
            'albany': 'NY',
            'jersey city': 'NJ',
            'hoboken': 'NJ',
            'montclair': 'NJ',
            'rahway': 'NJ',
            'clifton': 'NJ',
            'boonton': 'NJ',
            'west milford': 'NJ',
            'newark': 'NJ',
            'new jersey': 'NJ',
            'nj': 'NJ',
            'atlanta': 'ATL',
            'hiram': 'ATL',
            'south fulton': 'ATL',
            'georgia': 'ATL',
            'miami': 'MIA',
            'tampa': 'TPA',
            'orlando': 'ORL',
            'jacksonville': 'JAX',
            'sarasota': 'SRQ',
            'philadelphia': 'PHL',
            'birmingham': 'BHM',
            'birmingham, alabama': 'BHM',
            'nashville': 'NSH',
            'chicago': 'CHI',
            'chicago, il': 'CHI',
            'boston': 'BOS',
            'boston area': 'BOS',
            'government center': 'BOS',
            'san francisco': 'SF',
            'san jose': 'SJ',
            'san leandro': 'SF',
            'oakland': 'SF',
            'dublin': 'SF',
            'irvine': 'OC',
            'anaheim': 'OC',
            'costa mesa': 'OC',
            'santa rosa': 'OC',
            'north tustin': 'OC',
            'lake tahoe': 'CA',
            'thousands oaks': 'CA',
            'carson': 'CA',
            'norwalk': 'CA',
            'pomona': 'CA',
            'newburgh': 'NY',
            'stamford': 'CT',
            'new milford': 'CT',
            'san diego': 'SD',
            'indianapolis': 'IND',
            'kent': 'SEA',
            'seattle': 'SEA',
            'greensboro': 'NC',
            'washington': 'DC',
            'portland': 'PDX',
            'las vegas': 'LAS',
            'phoenix': 'PHX',
            'fort collins': 'CO',
            'louiseville': 'KY',
            'ann arbor': 'MI',
            'toronto': 'TOR',
            'vancouver': 'VAN',
            'mexico city': 'CDMX',
            'cdmx': 'CDMX',
            'sydney': 'SYD',
            'melbourne': 'MEL',
            'yarmouth': 'MA',
            'old westbury': 'NY',
            'other': 'OTHER',
            'not specified': 'UNKNOWN',
            'not explicitly stated': 'UNKNOWN',
            'virtual': 'REMOTE',
            'remote': 'REMOTE',
            'nationwide': 'USA',
            'nationwide usa': 'USA',
            'nationwide (usa)': 'USA'
        }
        
        # Project type mapping
        self.project_type_map = {
            'film': 'F',
            'movie': 'F',
            'feature': 'F',
            'short': 'F',
            'documentary': 'F',
            'miniseries': 'F',
            'mockumentary': 'F',

            'tv': 'T',
            'television': 'T',
            'series': 'T',
            'show': 'T',
            'streaming': 'T',
            'episodic': 'T',
            'pilot': 'T',

            'commercial': 'C',
            'ad': 'C',
            'advertisement': 'C',
            'campaign': 'C',
            'promo': 'C',
            'promotion': 'C',
            'ugc': 'C',
            'social': 'C',
            'internet commercial': 'C',
            'print': 'C',

            'voice': 'V',
            'voiceover': 'V',
            'voice over': 'V',
            'audio': 'V',
            'podcast': 'V',
            'radio': 'V',
            'music video': 'V',
            'video': 'V',
            'web': 'V',
            'digital': 'V',
            'virtual': 'V',
            'online': 'V',

            'industrial': 'I',
            'educational': 'I',
            'training': 'I',
            'reenactment': 'I',

            'theatre': 'R',
            'theater': 'R',
            'revival': 'R',
            'live event': 'R',
            'performance': 'R',
            'tour': 'R',

            'workshop': 'W',
            'background': 'W',
            'extras': 'W',
            'principal': 'W',

            'model': 'M',
            'photo': 'M',
            'photoshoot': 'M',
            'stock': 'M',
            'magazine': 'M',

            'student': 'S',
            'usc': 'S',

            'sag-aftra': 'U',
            'union': 'U',
            'non union': 'U',
            'agreement': 'U',

            'other': 'X',
            'project': 'X',
            'nan': 'X',
            ',': 'X'
        }
        
        # Lead/Principal role types
        self.lead_role_types = {
            'lead', 'principal', 'starring', 'star', 'main', 'protagonist',
            'hero', 'heroine', 'title', 'co-lead', 'co-star'
        }
        
        # Union affiliations
        self.union_keywords = {
            'sag', 'sag-aftra', 'aftra', 'aea'
        }
        
        # AI-related keywords for theme detection
        self.ai_keywords = {
            'ai', 'artificial intelligence', 'robot', 'android'
        }

    def extract_city_from_location(self, location_str):
        """Extract city from work_location string."""
        if pd.isna(location_str):
            return 'OTHER'
        
        location_str = str(location_str).lower().strip()
        
        # Try to match exact city names first
        for city, code in self.region_map.items():
            if city in location_str:
                return code
        
        # Default to OTHER if no match found
        return 'OTHER'

    def map_project_type(self, project_type_str):
        """Map project type to standardized codes."""
        if pd.isna(project_type_str):
            return 'V'  # Default to Voice/Other
        
        project_type_str = str(project_type_str).lower().strip()
        
        for keyword, code in self.project_type_map.items():
            if keyword in project_type_str:
                return code
        
        return 'V'  # Default to Voice/Other

    def is_lead_role(self, role_type_str, role_name_str):
        """Determine if a role is a lead/principal role."""
        if pd.isna(role_type_str) and pd.isna(role_name_str):
            return False
        
        combined_text = f"{str(role_type_str)} {str(role_name_str)}".lower()
        
        return any(keyword in combined_text for keyword in self.lead_role_types)

    def is_union_role(self, union_str):
        """Determine if a role is union-affiliated."""
        if pd.isna(union_str):
            return False
        
        union_str = str(union_str).lower()
        return any(keyword in union_str for keyword in self.union_keywords)

    def extract_rate_value(self, rate_str):
        """Extract numeric rate value from rate string."""
        if pd.isna(rate_str):
            return np.nan
        
        rate_str = str(rate_str)
        
        # Extract numbers with decimal points
        numbers = re.findall(r'\d+\.?\d*', rate_str)
        
        if numbers:
            # Take the first number found
            return float(numbers[0])
        
        return np.nan

    def calculate_sentiment(self, description_str):
        """Calculate sentiment score using TextBlob."""
        if pd.isna(description_str):
            return 0.0
        
        try:
            blob = TextBlob(str(description_str))
            # TextBlob returns polarity between -1 and 1
            return blob.sentiment.polarity
        except:
            return 0.0

    def has_ai_theme(self, text_str):
        """Check if text contains AI-related keywords."""
        if pd.isna(text_str):
            return False
        
        text_str = str(text_str).lower()
        return any(keyword in text_str for keyword in self.ai_keywords)

    def round_to_nearest_25(self, value):
        """Round value to nearest $25."""
        if pd.isna(value):
            return np.nan
        return round(value / 25) * 25

    def round_to_nearest_05(self, value):
        """Round value to nearest 0.05."""
        if pd.isna(value):
            return np.nan
        return round(value * 20) / 20

    def add_laplace_noise(self, value, scale=1.0):
        """Add Laplace noise for privacy protection."""
        if pd.isna(value):
            return value
        noise = np.random.laplace(0, scale)
        return max(0, value + noise)  # Ensure non-negative

    def process_data(self, df):
        """Process the raw breakdown data."""
        logger.info(f"Processing {len(df)} rows of breakdown data")
        
        # Create a copy to avoid modifying original
        processed_df = df.copy()
        
        # Extract date from posted_date
        processed_df['date_utc'] = pd.to_datetime(processed_df['posted_date']).dt.date
        
        # Map regions
        processed_df['region_code'] = processed_df['work_location'].apply(
            self.extract_city_from_location
        )
        
        # Map project types
        processed_df['proj_type_code'] = processed_df['project_type'].apply(
            self.map_project_type
        )
        
        # Determine lead roles
        processed_df['is_lead'] = processed_df.apply(
            lambda row: self.is_lead_role(row['role_type'], row['role_name']), axis=1
        )
        
        # Determine union status
        processed_df['is_union'] = processed_df['union'].apply(self.is_union_role)
        
        # Extract rate values
        processed_df['rate_value'] = processed_df['rate'].apply(self.extract_rate_value)
        
        # Calculate sentiment
        processed_df['sentiment'] = processed_df['role_description'].apply(
            self.calculate_sentiment
        )
        
        # Check for AI themes
        combined_text = (processed_df['role_description'].fillna('') + ' ' + 
                        processed_df['role_name'].fillna('') + ' ' +
                        processed_df['project_name'].fillna(''))
        processed_df['has_ai_theme'] = combined_text.apply(self.has_ai_theme)
        
        return processed_df

    def generate_pulse_report(self, processed_df):
        """Generate the daily pulse report."""
        logger.info("Generating daily pulse report")
        
        # Group by date, region, and project type
        grouped = processed_df.groupby(['date_utc', 'region_code', 'proj_type_code'])
        
        pulse_data = []
        
        for (date, region, proj_type), group in grouped:
            # Skip low-volume buckets for privacy (< 5 rows)
            if len(group) < 5:
                continue
            
            # Calculate metrics
            role_count = len(group)
            lead_count = group['is_lead'].sum()
            union_count = group['is_union'].sum()
            ai_count = group['has_ai_theme'].sum()
            
            # Calculate percentages
            lead_share_pct = round((lead_count / role_count) * 100, 1)
            union_share_pct = round((union_count / role_count) * 100, 1)
            theme_ai_share_pct = round((ai_count / role_count) * 100, 1)
            
            # Calculate median rate
            valid_rates = group['rate_value'].dropna()
            if len(valid_rates) > 0:
                median_rate = self.round_to_nearest_25(valid_rates.median())
            else:
                median_rate = np.nan
            
            # Calculate average sentiment
            valid_sentiments = group['sentiment'].dropna()
            if len(valid_sentiments) > 0:
                sentiment_avg = self.round_to_nearest_05(valid_sentiments.mean())
            else:
                sentiment_avg = 0.0
            
            # Add some privacy noise to percentages
            lead_share_pct = max(0, min(100, self.add_laplace_noise(lead_share_pct, 0.5)))
            union_share_pct = max(0, min(100, self.add_laplace_noise(union_share_pct, 0.5)))
            theme_ai_share_pct = max(0, min(100, self.add_laplace_noise(theme_ai_share_pct, 0.5)))
            
            pulse_data.append({
                'date_utc': date,
                'region_code': region,
                'proj_type_code': proj_type,
                'role_count_day': role_count,
                'lead_share_pct_day': round(lead_share_pct, 1),
                'union_share_pct_day': round(union_share_pct, 1),
                'median_rate_day_usd': median_rate,
                'sentiment_avg_day': sentiment_avg,
                'theme_ai_share_pct_day': round(theme_ai_share_pct, 1)
            })
        
        # Create DataFrame and sort
        pulse_df = pd.DataFrame(pulse_data)
        pulse_df = pulse_df.sort_values(['date_utc', 'region_code', 'proj_type_code'])
        
        logger.info(f"Generated pulse report with {len(pulse_df)} rows")
        return pulse_df

    def run(self, input_file, output_file):
        """Main execution function."""
        try:
            # Load data
            logger.info(f"Loading data from {input_file}")
            df = pd.read_csv(input_file)
            
            # Process data
            processed_df = self.process_data(df)
            
            # Generate pulse report
            pulse_df = self.generate_pulse_report(processed_df)
            
            # Save output
            logger.info(f"Saving pulse report to {output_file}")
            pulse_df.to_csv(output_file, index=False)
            
            # Print summary
            logger.info("Pulse report generation completed successfully!")
            logger.info(f"Input rows: {len(df)}")
            logger.info(f"Output rows: {len(pulse_df)}")
            logger.info(f"Date range: {pulse_df['date_utc'].min()} to {pulse_df['date_utc'].max()}")
            logger.info(f"Regions: {sorted(pulse_df['region_code'].unique())}")
            logger.info(f"Project types: {sorted(pulse_df['proj_type_code'].unique())}")
            
            return pulse_df
            
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Generate daily casting pulse report')
    parser.add_argument('--input', required=True, help='Input CSV file path')
    parser.add_argument('--output', required=True, help='Output CSV file path')
    
    args = parser.parse_args()
    
    # Create builder and run
    builder = CastingPulseBuilder()
    pulse_df = builder.run(args.input, args.output)
    
    # Display sample of output
    print("\nSample output:")
    print(pulse_df.head())

if __name__ == "__main__":
    main()