import time
import requests
from config.loader import settings
from data.repositories import SolarSiteRepository
from utils.logger_config import get_logger
from utils.json_parser import tolerant_json_decode

logger = get_logger(__name__)

API_CONF = settings.api
RETRY_DELAY = API_CONF.retry_delay
MAX_RETRIES = API_CONF.max_retries

class APIService:
    """
    Handles fetching data from the Solar API.
    """
    def __init__(self, base_url: str, headers: dict, timeout: int = 30):
        self.base_url = base_url
        self.headers = headers
        self.timeout = timeout

    def fetch_data(self, start: int, limit: int) -> dict | None:
        """
        Fetches a batch of data from the API.
        Retries on failure up to MAX_RETRIES.
        """
        params = {
            'start': start,
            'limit': limit,
            'sort': 'maxImpact',
            'dir': 'ASC',
            'status': 0,
            'category': 0,
            'filter': '',
            'showMap': 'false'
        }
        for attempt in range(MAX_RETRIES):
            try:
                logger.debug("Requesting API: %s with params: %s", self.base_url, params)
                response = requests.get(
                    self.base_url, params=params, headers=self.headers, timeout=self.timeout
                )
                response.raise_for_status()
                data = tolerant_json_decode(response.text)
                if data is None:
                    logger.error(
                        "Failed to decode JSON response from API after all attempts (attempt %d/%d). URL: %s, Params: %s",
                        attempt + 1, MAX_RETRIES, self.base_url, params
                    )
                    # Optionally, log part of the response text if data is None
                    logger.debug("Response text (first 500 chars): %s", response.text[:500])
                return data
            except requests.exceptions.RequestException as e:
                logger.error(
                    "API request failed (attempt %d/%d): %s. URL: %s, Params: %s",
                    attempt + 1, MAX_RETRIES, e, self.base_url, params
                )
                if attempt < MAX_RETRIES - 1:
                    logger.info("Retrying in %d seconds...", RETRY_DELAY)
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error("Max retries reached for API request. Giving up.")
                    return None
        return None


class DataProcessor:
    """
    Processes records and stores them in the database.
    """
    def __init__(self, repository: SolarSiteRepository):
        self.repo = repository

    def process_and_store_records(self, records_list: list) -> tuple[int, int]:
        """
        Processes a list of records and adds/updates them in the database.
        Returns a tuple of (processed_successfully_count, failed_to_process_count).
        """
        if not records_list:
            logger.info("No records to process.")
            return 0, 0

        processed_successfully_count = 0
        failed_to_process_count = 0

        for record in records_list:
            site_data = {
                'site_id': record.get('id'),
                'name': record.get('urlName'),
                'type': record.get('type'),
                'status': record.get('status'),
                'last_reporting_time': record.get('lastReportingTime'),
                'installation_date': record.get('installationDate'),
                'country': record.get('country'),
                'state': record.get('state'),
                'location': record.get('location'),
                'peak_power': record.get('peakPower'),
                'address': record.get('address'),
                'secondary_address': record.get('secondaryAddress'),
                'city': record.get('city'),
                'zip_code': record.get('zip'),
            }
            if not site_data['site_id']:
                logger.warning("Skipping record due to missing ID: %s", record)
                failed_to_process_count += 1
                continue
            try:
                self.repo.add_or_update(site_data)
                processed_successfully_count += 1
            except Exception as e:
                logger.error("Failed to store record %s: %s", site_data.get('site_id', 'Unknown ID'), e)
                failed_to_process_count += 1
        
        logger.info(
            "Processed batch: %d successfully, %d failed.",
            processed_successfully_count, failed_to_process_count
        )
        return processed_successfully_count, failed_to_process_count


class SolarDataImporter:
    """
    Orchestrates the fetching and processing of solar data.
    """
    def __init__(self, api_service: APIService, data_processor: DataProcessor):
        self.api_service = api_service
        self.data_processor = data_processor
        self.default_limit_per_request = API_CONF.default_limit
        self.max_consecutive_empty_batches = API_CONF.get('max_consecutive_empty_batches', 3)


    def import_data(self, max_total_records_to_fetch: int | None = None) -> None:
        """
        Main method to import solar data.
        Fetches data in batches, processes, and stores it.
        Stops if max_total_records_to_fetch is reached, if the API indicates no more data,
        or if too many consecutive empty batches are received.
        """
        start_index = 0
        total_records_fetched_session = 0
        consecutive_empty_batches = 0

        logger.info("Starting solar data import process...")

        while True:
            if max_total_records_to_fetch is not None and \
               total_records_fetched_session >= max_total_records_to_fetch:
                logger.info(
                    "Reached the configured maximum of %d records to fetch for this session. Stopping.",
                    max_total_records_to_fetch
                )
                break

            logger.info(
                "Fetching data batch starting at index %d, limit %d.",
                start_index, self.default_limit_per_request
            )
            api_response = self.api_service.fetch_data(start_index, self.default_limit_per_request)

            if not api_response:
                logger.error("Failed to fetch data or received empty response from API. Stopping import.")
                break

            if 'records' not in api_response:
                logger.error("API response is invalid or missing 'records' key. Response: %s. Stopping.", api_response)
                break
            
            current_records = api_response['records']
            api_total_count = api_response.get('totalCount', -1) # Total records available from API
            current_batch_size = len(current_records)
            
            logger.info(
                "Fetched batch of %d records. API reports total of %s records.",
                current_batch_size, api_total_count if api_total_count != -1 else "unknown"
            )

            if current_batch_size == 0:
                consecutive_empty_batches += 1
                logger.info(
                    "Fetched 0 records in this batch. Consecutive empty batches: %d/%d.",
                    consecutive_empty_batches, self.max_consecutive_empty_batches
                )
                if consecutive_empty_batches >= self.max_consecutive_empty_batches:
                    logger.warning(
                        "Stopping data import due to %d consecutive empty batches.",
                        self.max_consecutive_empty_batches
                    )
                    break
            else:
                consecutive_empty_batches = 0  # Reset counter
                processed_count, failed_count = self.data_processor.process_and_store_records(current_records)
                total_records_fetched_session += processed_count # Only count successfully processed ones
                logger.info(
                    "Successfully processed and stored/updated %d records from this batch. %d failed.",
                    processed_count, failed_count
                )

            start_index += self.default_limit_per_request # Increment for next batch

            # Exit condition: if we've fetched beyond the API's reported total count
            if api_total_count != -1 and start_index >= api_total_count and api_total_count > 0:
                logger.info(
                    "Fetched all available records (%d) according to API's totalCount. Stopping.",
                    total_records_fetched_session
                )
                break
            
            # Exit condition: if API reports 0 total and we got an empty batch (already handled by consecutive_empty_batches)
            if api_total_count == 0 and current_batch_size == 0:
                 logger.info("API reports 0 total records and current batch is empty. Stopping.")
                 break


            time.sleep(API_CONF.get('request_delay_seconds', 0.01)) # Small delay between requests

        logger.info(
            "Data import process finished. Total records successfully processed in this session: %d",
            total_records_fetched_session
        )


# For potential direct execution or use from other modules
def import_solar_data(max_total_records_to_fetch: int | None = None):
    """
    Initializes services and starts the data import process.
    This function can be called from main.py or other scripts.
    """
    logger.info("Initializing services for solar data import...")
    
    # Initialize repository
    repo = SolarSiteRepository()
    
    # Initialize APIService
    api_service = APIService(
        base_url=API_CONF.base_url,
        headers=dict(API_CONF.headers) # Ensure headers is a new dict
    )
    
    # Initialize DataProcessor
    data_processor = DataProcessor(repository=repo)
    
    # Initialize SolarDataImporter
    importer = SolarDataImporter(
        api_service=api_service,
        data_processor=data_processor
    )
    
    logger.info("Starting solar data import...")
    importer.import_data(max_total_records_to_fetch=max_total_records_to_fetch)
    logger.info("Solar data import process completed.")
