import datetime
import logging
import re
import pandas as pd
from peewee import fn

from data.models import db, SolarSite, SiteProductionData, SiteReferenceYearProduction
from utils.logger_config import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

# Define a reference year (e.g., 2000) for normalizing timestamps
# This ensures leap years are handled consistently if we pick one.
# Using a non-leap year like 2001 might be simpler to avoid Feb 29 issues
# if the source data doesn't always align with leap years.
# For now, let's use 2000, which is a leap year.
REFERENCE_YEAR = 2000

def parse_peak_power(peak_power_str: str) -> float | None:
    """
    Parses a peak_power string (e.g., "9.87") which is assumed to be in kW,
    and returns power in Watts.
    Returns None if parsing fails.
    """
    if not peak_power_str:
        logger.debug("peak_power_str is None or empty.")
        return None
    try:
        # Assuming the string is a direct representation of kW
        kw_value = float(peak_power_str)
        return kw_value * 1000  # Convert kW to Watts
    except ValueError:
        logger.warning(f"Could not parse peak_power string '{peak_power_str}' as a numeric kW value. It might contain units or other text.")
        # Fallback to original regex parsing if direct float conversion fails
        # This handles cases like "4.5kWp" or "5000 W" if they exist.
        cleaned_str = peak_power_str.lower()
        kw_match = re.search(r'([\d\.]+)\s*k', cleaned_str) # Match 'k' for kW, kWp etc.
        if kw_match:
            try:
                return float(kw_match.group(1)) * 1000
            except ValueError:
                pass
        
        w_match = re.search(r'([\d\.]+)\s*w', cleaned_str) # Match 'w' for W
        if w_match:
            try:
                return float(w_match.group(1))
            except ValueError:
                pass
        
        logger.error(f"Failed to parse peak_power: '{peak_power_str}' using all methods.")
        return None

def calculate_and_store_yearly_profiles():
    """
    Calculates and stores the 15-minute interval average "per kW generation"
    for a representative year for each solar site.
    """
    logger.info("Starting yearly profile calculation process.")
    
    sites = SolarSite.select().where(
               SolarSite.uploaded_on.is_null(False),
                SolarSite.profile_updated_on.is_null(True) 
        )
    
    for site in sites:
        logger.info(f"Processing site ID: {site.site_id}, Name: {site.name}")
        
        peak_power_watts = parse_peak_power(site.peak_power)
        if peak_power_watts is None or peak_power_watts == 0:
            logger.warning(f"Site ID: {site.site_id} has invalid or zero peak power ({site.peak_power}). Skipping.")
            continue
            
        # Fetch production data for the site
        production_records = (SiteProductionData
                              .select(SiteProductionData.timestamp, SiteProductionData.production)
                              .where(SiteProductionData.site == site)
                              .order_by(SiteProductionData.timestamp)
                              .tuples())
        
        if not production_records:
            logger.info(f"No production data found for site ID: {site.site_id}. Skipping.")
            continue
            
        df = pd.DataFrame(list(production_records), columns=['timestamp', 'production'])
        
        # Ensure production is numeric and handle potential errors
        df['production'] = pd.to_numeric(df['production'], errors='coerce')
        df.dropna(subset=['production'], inplace=True) # Remove rows where production couldn't be coerced

        if df.empty:
            logger.info(f"No valid production data after cleaning for site ID: {site.site_id}. Skipping.")
            continue

        # --- Data Cleaning & Preparation ---
        # 1. Filter out days where total production is zero
        df['date'] = df['timestamp'].dt.date
        daily_production = df.groupby('date')['production'].sum()
        valid_dates = daily_production[daily_production > 0].index
        # Ensure df_filtered is a copy to avoid SettingWithCopyWarning
        df_filtered = df[df['date'].isin(valid_dates)].copy()
        
        if df_filtered.empty:
            logger.info(f"No days with production > 0 for site ID: {site.site_id}. Skipping.")
            continue
            
        # 2. Check for 12 months of data (can be across different years)
        df_filtered['month'] = df_filtered['timestamp'].dt.month
        unique_months = df_filtered['month'].nunique()
        if unique_months < 12:
            logger.info(f"Site ID: {site.site_id} has data for {unique_months}/12 months. Skipping (incomplete yearly data).")
            continue
            
        logger.info(f"Site ID: {site.site_id} has {len(df_filtered)} valid production records across {unique_months} months.")

        # --- Normalize Data & Calculate Averages ---
        # Create a 'time_of_day' column (HH:MM) for grouping
        df_filtered['time_of_day'] = df_filtered['timestamp'].dt.time
        
        # Group by month, day, and time_of_day to average across years for the same calendar day and time
        # Then, normalize the timestamp to the REFERENCE_YEAR
        # We want an average for each 15-min interval of a "typical" year.
        
        # Create a normalized timestamp for grouping by interval in a generic year
        # This helps average Jan 1st 00:00 from 2022, 2023, etc.
        df_filtered['normalized_timestamp'] = df_filtered['timestamp'].apply(
            lambda ts: ts.replace(year=REFERENCE_YEAR)
        )
        
        # Group by this normalized timestamp and calculate mean production
        # This gives the average production for each 15-min slot of the reference year
        averaged_production = df_filtered.groupby('normalized_timestamp')['production'].mean().reset_index()
        
        if averaged_production.empty:
            logger.info(f"No data after averaging for site ID: {site.site_id}. Skipping.")
            continue

        # Calculate "per kW generation"
        # Production is in Watts, peak_power_watts is in Watts. Result is W/W = unitless ratio.
        # If you want kWh/kW, then production needs to be in kWh (i.e. production_Wh / 1000)
        # The user asked for "per kW generation = 3223/4500 = 0.716kW or 716 Watts"
        # This implies the result should be a ratio (kW/kW or W/W).
        # If production is instantaneous power (W) and capacity is in W, then W/W is correct.
        averaged_production['per_kw_generation'] = averaged_production['production'] / peak_power_watts
        
        # --- Store Profile ---
        profile_data_to_insert = []
        for _, row in averaged_production.iterrows():
            profile_data_to_insert.append({
                'site': site,
                'reference_timestamp': row['normalized_timestamp'],
                'per_kw_generation': row['per_kw_generation']
            })
            
        if profile_data_to_insert:
            with db.atomic():
                # Clear old profile data for this site
                SiteReferenceYearProduction.delete().where(SiteReferenceYearProduction.site == site).execute()
                # Insert new profile data
                SiteReferenceYearProduction.insert_many(profile_data_to_insert).execute()
            
            site.profile_updated_on = datetime.datetime.now()
            site.save()
            logger.info(f"Successfully calculated and stored yearly profile for site ID: {site.site_id}. {len(profile_data_to_insert)} intervals.")
        else:
            logger.info(f"No profile data generated to store for site ID: {site.site_id}.")

    logger.info("Finished yearly profile calculation process.")

if __name__ == "__main__":
    try:
        db.connect(reuse_if_open=True)
        calculate_and_store_yearly_profiles()
    except Exception as e:
        logger.error(f"An error occurred during the profile calculation: {e}", exc_info=True)
    finally:
        if not db.is_closed():
            db.close()
        logger.info("Database connection closed.")
