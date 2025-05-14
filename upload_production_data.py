import os
import csv
from datetime import datetime, timezone
import peewee
from data.models import SolarSite, SiteProductionData, db
from utils.logger_config import get_logger # Changed import
from config.loader import settings

logger = get_logger(__name__) # Create logger instance
CSV_DIRECTORY = 'csv_data'

def parse_production_value(value_str):
    """
    Parses the production value string, removing quotes and converting to int.
    Handles cases where value might be missing or not a valid number.
    """
    try:
        # Remove common quote characters and whitespace
        cleaned_value = value_str.replace('"', '').strip()
        if not cleaned_value: # Handle empty strings after cleaning
            return 0
        return int(float(cleaned_value)) # float first to handle "0.0" then int
    except ValueError:
        logger.warning(f"Could not parse production value: '{value_str}'. Defaulting to 0.")
        return 0

def upload_csv_data():
    """
    Scans the CSV_DIRECTORY for site production CSV files,
    parses them, and uploads the data to the database.
    Updates the uploaded_on timestamp for the site.
    """
    logger.info("Starting CSV data upload process.")
    if not os.path.exists(CSV_DIRECTORY):
        logger.error(f"CSV directory '{CSV_DIRECTORY}' not found.")
        return

    processed_files = 0
    total_rows_imported = 0

    try:
        db.connect(reuse_if_open=True)
        logger.info("Database connection established.")
        
        # Query sites from DB that have CSVs and have an existing uploaded_on date (uploaded_on IS NOT NULL)
        sites_to_process = SolarSite.select().where(
            SolarSite.has_csv == True,
            SolarSite.uploaded_on.is_null(True) # Ensure uploaded_on is NULL
        )

        logger.info(f"Found {len(sites_to_process)} sites in DB marked with has_csv=true and uploaded_on IS NOT NULL.")

        for solar_site in sites_to_process:
            site_id = solar_site.site_id
            file_path_to_process = None
            expected_prefix = f"{site_id}_"

            # Search for the CSV file recursively
            found_file = False
            for root, _, files in os.walk(CSV_DIRECTORY):
                for filename in files:
                    if filename.startswith(expected_prefix) and filename.endswith('.csv'):
                        file_path_to_process = os.path.join(root, filename)
                        logger.info(f"Found matching CSV file: {file_path_to_process} for site ID {site_id}")
                        found_file = True
                        break # Found the file for this site_id in this directory
                if found_file:
                    break # Stop searching further directories for this site_id
            
            if not file_path_to_process:
                logger.warning(f"No CSV file starting with '{expected_prefix}' and ending with '.csv' found for site ID {site_id} in {CSV_DIRECTORY} or its subdirectories. Skipping.")
                continue

            logger.info(f"Processing file: {file_path_to_process} for site ID {site_id}")
            # Ensure file_path variable used below is file_path_to_process
            # The variable name change is mostly for clarity in this block
            # For consistency in the rest of the loop, I'll assign it back to file_path
            file_path = file_path_to_process

            production_data_batch = []
            # rows_in_file = 0 # Not strictly needed for logic, but can be kept for logging
            try:
                with open(file_path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    header = next(reader, None)

                    if not header or not all(h in header for h in ["Time", "System Production (W)"]):
                        logger.warning(f"File {file_path} has missing 'Time' or 'System Production (W)' columns in header. Header: {header}. Skipping.")
                        continue
                    
                    time_col_idx = header.index("Time")
                    prod_col_idx = header.index("System Production (W)")

                    for row_num, row in enumerate(reader, start=1):
                        if len(row) <= max(time_col_idx, prod_col_idx):
                            logger.warning(f"Skipping malformed row {row_num} in {file_path}: {row}. Not enough columns.")
                            continue
                        
                        time_str = row[time_col_idx]
                        production_str = row[prod_col_idx]

                        try:
                            timestamp_obj = datetime.strptime(time_str, '%m/%d/%Y %H:%M')
                            timestamp_obj_utc = timestamp_obj.replace(tzinfo=timezone.utc)
                        except ValueError:
                            logger.warning(f"Could not parse timestamp: '{time_str}' in {file_path}, row {row_num}. Skipping row.")
                            continue
                        
                        production_value = parse_production_value(production_str)

                        production_data_batch.append({
                            'site': solar_site, # Use the SolarSite instance directly
                            'timestamp': timestamp_obj_utc,
                            'production': production_value
                        })
                        # rows_in_file += 1
                
                if production_data_batch:
                    with db.atomic():
                        SiteProductionData.insert_many(production_data_batch).on_conflict_ignore().execute()
                    total_rows_imported += len(production_data_batch)
                    logger.info(f"Imported {len(production_data_batch)} records from {file_path} for site {site_id}.")
                    
                    solar_site.uploaded_on = datetime.now(timezone.utc)
                    solar_site.save()
                    logger.info(f"Updated uploaded_on for site {site_id}.")
                    processed_files += 1
                else:
                    logger.info(f"No data to import from {file_path}.")

            except FileNotFoundError:
                logger.error(f"File not found during processing (should have been caught earlier): {file_path}. This is unexpected.")
            except Exception as e:
                logger.error(f"Error processing file {file_path} for site ID {site_id}: {e}")

    except peewee.PeeweeException as e:
        logger.error(f"Database error during CSV upload process: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if not db.is_closed():
            db.close()
            logger.info("Database connection closed.")
    
    logger.info(f"CSV data upload process finished. Processed {processed_files} files. Imported {total_rows_imported} new production records.")


if __name__ == "__main__":
    # Ensure the script can find other modules if run directly
    import sys
    # Add project root to sys.path if necessary, assuming script is in project root
    # sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    logger.info("Starting production data upload script.")
    upload_csv_data()
    logger.info("Production data upload script finished.")
