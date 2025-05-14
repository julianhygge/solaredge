import logging
import requests
import json
import time
import re
import html
import logging
from config.loader import settings
from data.repositories import SolarSiteRepository

import json


def fix_invalid_json(text):
    # Remove JS-style fields (e.g., viewDashboard:true, ...)
    text = re.sub(r'\s*view[A-Za-z]+\s*:\s*[^,\n]+,?', '', text)
    # Remove any leftover JS boolean expressions (e.g., true && false && true,)
    text = re.sub(r'\s*:\s*true\s*&&.*?,', ': false,', text)
    # Fix invalid backslashes
    text = re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', text)
    # Decode HTML entities
    text = html.unescape(text)
    return text


def tolerant_json_decode(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError as e1:
        logging.error("Standard JSON decode failed: %s", e1)
        try:
            cleaned = fix_invalid_json(text)
            return json.loads(cleaned)
        except json.JSONDecodeError as e2:
            logging.error("Cleaned JSON decode failed: %s", e2)
            try:
                import demjson3
                return demjson3.decode(text)
            except Exception as e3:
                logging.error("demjson3 decode failed: %s", e3)
                logging.error(
                    "Response text snippet (first 800 chars): %s", text[:800])
                return None


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

API_CONF = settings.api
RETRY_DELAY = API_CONF.retry_delay
MAX_RETRIES = API_CONF.max_retries

repo = SolarSiteRepository()


def fetch_data_from_api(start, limit):
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
            # Print headers for debugging
            # Use all headers from config
            headers = dict(API_CONF.headers)
            logging.info("Request headers: %s", headers)
            response = requests.get(
                API_CONF.base_url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            try:
                # Debug: print first record if present
                try:
                    data = response.json()
                    if 'records' in data and data['records']:
                        logging.info("First record in API response: %s", data['records'][0])
                except Exception as e:
                    logging.warning("Could not print first record for debug: %s", e)
                return response.json()
            except json.JSONDecodeError as json_err:
                logging.error("JSON decoding failed: %s", json_err)
                snippet = response.text[:500]
                logging.info(
                    "Response text snippet (first 500 chars): %s", snippet)
                return tolerant_json_decode(response.text)
        except requests.exceptions.RequestException as e:
            logging.error(
                "API request failed (attempt %d/%d): %s", attempt + 1, MAX_RETRIES, e)
            if attempt < MAX_RETRIES - 1:
                logging.info("Retrying in %d seconds...", RETRY_DELAY)
                time.sleep(RETRY_DELAY)
            else:
                return None


def process_and_store_records(records_list):
    if not records_list:
        logging.info("No records to process.")
        return 0, 0
    processed_successfully_count = 0
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
            logging.warning(f"Skipping record due to missing ID: {record}")
            continue
        repo.add_or_update(site_data)
        processed_successfully_count += 1
    return processed_successfully_count, 0


def import_solar_data(max_total_records_to_fetch=None):
    start_index = 0
    limit_per_request = API_CONF.default_limit
    total_records_fetched_session = 0
    total_api_count = -1
    while True:
        api_response = fetch_data_from_api(start_index, limit_per_request)
        if not api_response or 'records' not in api_response:
            logging.info("No more records returned from the API.")
            break
        current_records = api_response['records']
        total_api_count = api_response.get('totalCount', -1)
        current_batch_count = len(current_records)
        total_records_fetched_session += current_batch_count
        if not current_records:
            logging.info("No more records returned from the API.")
            break
        logging.info("Fetched %d records in this batch.", current_batch_count)
        processed_count, _ = process_and_store_records(current_records)
        logging.info(
            "Successfully processed and stored/updated %d records from this batch.", processed_count)
        start_index += limit_per_request
        if max_total_records_to_fetch is not None and total_records_fetched_session >= max_total_records_to_fetch:
            logging.info(
                "Reached the configured maximum of %d records to fetch for this session.", max_total_records_to_fetch)
            break
        if total_api_count > 0 and start_index >= total_api_count:
            logging.info(
                "Fetched all available records according to API's totalCount.")
            break
        time.sleep(1)
    logging.info(
        "Data import process finished. Total records fetched in this session: %d",
        total_records_fetched_session)
