import os
import requests
import logging
from datetime import datetime, timezone
import re

from data.models import SolarSite, db

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DOWNLOAD_URL = "https://monitoringpublic.solaredge.com/solaredge-web/p/charts/{site_id}/chartExport"
CSV_BASE_DIR = "csv_data"

def parse_date_string(date_str):
    """
    Parses a date string into a datetime object.
    Tries a few common formats. Add more formats if needed.
    Returns None if parsing fails.
    """
    if not date_str:
        return None
    formats_to_try = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y", # Added for MM/DD/YYYY format
        "%m/%d/%Y %H:%M", # Added for MM/DD/YYYY HH:MM format
        # Add other potential formats from SolarEdge API if known
    ]
    for fmt in formats_to_try:
        try:
            # Attempt to parse, assuming UTC if no timezone info
            dt = datetime.strptime(date_str, fmt)
            # If the source data might not be UTC, consider localizing or making it timezone-aware differently
            # For now, let's assume if it's naive, it's UTC for consistency in timestamp generation
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    logging.warning(f"Could not parse date string: {date_str} with known formats.")
    return None

def datetime_to_ms_timestamp(dt_obj):
    """Converts a datetime object to milliseconds since Unix epoch."""
    if not dt_obj:
        return None
    # Ensure datetime is UTC before converting
    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    else:
        dt_obj = dt_obj.astimezone(timezone.utc)
    return int(dt_obj.timestamp() * 1000)

def sanitize_filename_part(part):
    """Sanitizes a string to be used as part of a filename."""
    if not part:
        return "unknown"
    # Remove or replace characters not suitable for filenames
    part = str(part)
    part = re.sub(r'[^\w\s-]', '', part).strip() # Keep alphanumeric, whitespace, hyphens
    part = re.sub(r'[-\s]+', '-', part) # Replace spaces/multiple hyphens with single hyphen
    return part if part else "unknown"

def download_csvs_for_sites():
    """
    Downloads CSV data for solar sites based on their installation_date,
    last_reporting_time, and updated_on fields.
    """
    try:
        db.connect(reuse_if_open=True)
        countries_to_filter = ['India']
        sites = SolarSite.select().where(
            (SolarSite.country.in_(countries_to_filter)) &
            (SolarSite.has_csv == False)
        )
        logging.info(f"Querying sites for countries: {countries_to_filter} where has_csv is False.")

        if not sites:
            logging.info(f"No sites found in the database for countries: {countries_to_filter} with has_csv = False.")
            return

        for site in sites:
            logging.info(f"Processing site ID: {site.site_id}, Name: {site.name}")

            # 1. Determine start_time (st)
            start_date_dt = None
            if site.updated_on:
                start_date_dt = site.updated_on # This should already be a datetime object from Peewee
                # Ensure it's timezone-aware (UTC)
                if start_date_dt.tzinfo is None:
                     start_date_dt = start_date_dt.replace(tzinfo=timezone.utc)
                logging.info(f"Using updated_on for start_date: {start_date_dt}")
            elif site.installation_date:
                start_date_dt = parse_date_string(site.installation_date)
                logging.info(f"Using installation_date for start_date: {start_date_dt} (parsed from {site.installation_date})")
            
            if not start_date_dt:
                logging.warning(f"Site ID {site.site_id}: Skipping. Cannot determine start date (updated_on and installation_date are missing or invalid).")
                continue

            # 2. Determine end_time (et)
            end_date_dt = parse_date_string(site.last_reporting_time)
            if not end_date_dt:
                logging.warning(f"Site ID {site.site_id}: Skipping. Cannot parse last_reporting_time: {site.last_reporting_time}")
                continue
            
            logging.info(f"Determined start_date_dt: {start_date_dt}, end_date_dt: {end_date_dt}")

            # 3. Condition: if last_reporting_time < updated_on (or installation_date if updated_on is null), don't make request
            # Note: start_date_dt is already timezone-aware (UTC). end_date_dt is also made UTC by parse_date_string.
            if end_date_dt < start_date_dt:
                logging.info(f"Site ID {site.site_id}: Skipping. last_reporting_time ({end_date_dt}) is earlier than start_date ({start_date_dt}). No new data to fetch.")
                continue

            st_ms = datetime_to_ms_timestamp(start_date_dt)
            et_ms = datetime_to_ms_timestamp(end_date_dt)

            if st_ms is None or et_ms is None:
                logging.error(f"Site ID {site.site_id}: Failed to convert dates to timestamps.")
                continue

            # 4. Construct URL
            # Example: https://monitoringpublic.solaredge.com/solaredge-web/p/charts/2711994/chartExport?st=1747180800000&et=1747267199000&fid=2711994&timeUnit=2&pn0=Power&id0=0&t0=0&hasMeters=false
            params = {
                "st": st_ms,
                "et": et_ms,
                "fid": site.site_id,
                "timeUnit": 2, # As per example URL
                "pn0": "Power", # As per example URL
                "id0": 0, # As per example URL
                "t0": 0, # As per example URL
                "hasMeters": "false" # As per example URL
            }
            download_url = BASE_DOWNLOAD_URL.format(site_id=site.site_id)
            
            logging.info(f"Site ID {site.site_id}: Download URL: {download_url} with params: {params}")

            # 5. Create directory structure: csv_data/<country>/<state>/<city>/
            country_dir = sanitize_filename_part(site.country)
            state_dir = sanitize_filename_part(site.state)
            city_dir = sanitize_filename_part(site.city)
            
            target_dir = os.path.join(CSV_BASE_DIR, country_dir, state_dir, city_dir)
            os.makedirs(target_dir, exist_ok=True)

            # 6. Construct filename: <site_id>_<name>.csv
            site_name_sanitized = sanitize_filename_part(site.name)
            filename = f"{site.site_id}_{site_name_sanitized}.csv"
            filepath = os.path.join(target_dir, filename)

            # 7. Download CSV
            try:
                response = requests.get(download_url, params=params, timeout=300) # Increased to 300 seconds (5 minutes)
                response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
                
                with open(filepath, 'wb') as f: # write in binary mode
                    f.write(response.content)
                logging.info(f"Site ID {site.site_id}: Successfully downloaded CSV to {filepath}")

                # 8. Update SolarSite record
                site.has_csv = True
                site.updated_on = datetime.now(timezone.utc) # Current timestamp in UTC
                site.save()
                logging.info(f"Site ID {site.site_id}: Database record updated. has_csv=True, updated_on={site.updated_on}")

            except requests.exceptions.RequestException as e:
                logging.error(f"Site ID {site.site_id}: Failed to download CSV. Error: {e}")
            except IOError as e:
                logging.error(f"Site ID {site.site_id}: Failed to save CSV to {filepath}. Error: {e}")
            except Exception as e:
                logging.error(f"Site ID {site.site_id}: An unexpected error occurred. Error: {e}")
                
    except Exception as e:
        logging.error(f"An error occurred in the main download process: {e}")
    finally:
        if not db.is_closed():
            db.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    logging.info("Starting CSV download process...")
    download_csvs_for_sites()
    logging.info("CSV download process finished.")
