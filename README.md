# SolarEdge Data Importer

This project is designed to interact with the SolarEdge monitoring API to fetch, store, and process solar panel installation data. It includes functionalities to:

1.  **Import Site Data**: Fetch solar site information from the SolarEdge API and store it in a PostgreSQL database.
2.  **Download CSV Production Data**: Download energy production CSV files for specific sites.
3.  **Upload Production Data**: Parse the downloaded CSV files and upload the production metrics into the database.

## Project Structure

```
.
├── .gitignore
├── config/
│   ├── __init__.py
│   ├── loader.py         # Loads configuration using Dynaconf
│   └── settings.toml     # Configuration file for database, API, etc.
├── csv_data/             # Directory where downloaded CSVs are stored (dynamically created)
├── data/
│   ├── __init__.py
│   ├── models.py         # Peewee ORM models for database tables (SolarSite, SiteProductionData)
│   └── repositories.py   # Data access layer for SolarSite
├── domain/
│   ├── __init__.py
│   └── sites_importer_service.py # Core logic for fetching and processing site data
├── download_site_csvs.py # Script to download CSV production data for sites
├── import_solaredge_sites.py # Main script to trigger the import of site data
├── requirements.txt      # Python package dependencies
├── upload_production_data.py # Script to upload data from CSVs to the database
├── calculate_yearly_profiles.py # Script to calculate normalized yearly generation profiles
└── utils/
    ├── __init__.py
    ├── json_parser.py    # Utility for robust JSON parsing
    └── logger_config.py  # Logging configuration
```

## Core Components

### 1. Configuration (`config/`)

*   **`settings.toml`**: Contains all configurations for the application, including:
    *   PostgreSQL database connection details (host, port, user, password, database name, schema).
    *   SolarEdge API details (base URL, default parameters, headers, retry logic).
*   **`loader.py`**: Uses `Dynaconf` to load settings from `settings.toml`, making them accessible throughout the application.

### 2. Data Layer (`data/`)

*   **`models.py`**: Defines the database schema using Peewee ORM.
    *   `SolarSite`: Represents a solar installation site with attributes like `site_id`, `name`, `status`, `location`, `installation_date`, `last_reporting_time`, `has_csv`, `updated_on`, `uploaded_on`, `profile_updated_on`, etc.
    *   `SiteProductionData`: Stores time-series energy production data for each site, linked to `SolarSite`. Includes `timestamp` and `production` value.
    *   `SiteReferenceYearProduction`: Stores the calculated 15-minute interval average "per kW generation" for a representative year for each site. Linked to `SolarSite`.
    *   Establishes a connection to the PostgreSQL database using credentials from `settings.toml`.
*   **`repositories.py`**:
    *   `SolarSiteRepository`: Provides methods to interact with the `SolarSite` table (e.g., `add_or_update`, `get_all`, `get_by_id`).

### 3. Domain Logic (`domain/`)

*   **`sites_importer_service.py`**: Contains the primary business logic.
    *   `APIService`: Handles communication with the SolarEdge API. It fetches site data in batches, with retry mechanisms for network issues.
    *   `DataProcessor`: Takes the raw data from the API, transforms it into the `SolarSite` model structure, and uses `SolarSiteRepository` to save it to the database.
    *   `SolarDataImporter`: Orchestrates the import process. It uses `APIService` to fetch data and `DataProcessor` to store it. It manages pagination and can limit the total number of records fetched.
    *   `import_solar_data()`: A convenience function to initialize and run the import process.

### 4. Utility Functions (`utils/`)

*   **`logger_config.py`**: Sets up application-wide logging to standard output.
*   **`json_parser.py`**: Provides a `tolerant_json_decode` function that attempts to parse JSON, with fallbacks and cleaning mechanisms for malformed JSON responses from the API. It can use `demjson3` as a last resort.

### 5. Main Scripts

*   **`import_solaredge_sites.py`**:
    *   The entry point for fetching and storing solar site metadata from the SolarEdge API.
    *   Calls `import_solar_data()` from `sites_importer_service.py`.
    *   Can be configured to fetch all available records or a limited number for testing.

*   **`download_site_csvs.py`**:
    *   Connects to the database to find sites (e.g., in 'India' where `has_csv` is `False`).
    *   Constructs download URLs for SolarEdge's CSV export endpoint based on site ID and date ranges (`updated_on` or `installation_date` as start, `last_reporting_time` or current time as end).
    *   Downloads CSV files containing production data.
    *   Saves CSVs into a structured directory: `csv_data/<country>/<state>/<city>/<site_id>_<name>.csv`.
    *   Updates the `SolarSite` record to set `has_csv = True` and updates `updated_on` after successful download.

*   **`upload_production_data.py`**:
    *   Scans the `csv_data/` directory for CSV files corresponding to sites marked with `has_csv = True` and `uploaded_on IS NULL` in the database.
    *   Parses each CSV file, extracting "Time" and "System Production (W)" columns.
    *   Converts timestamps to UTC and production values to integers.
    *   Batch-inserts the production data into the `SiteProductionData` table, avoiding duplicates based on site and timestamp.
    *   Updates the `uploaded_on` timestamp for the `SolarSite` after its data is successfully imported.

## Dependencies

The project relies on the following Python libraries (see `requirements.txt`):

*   `requests`: For making HTTP requests to the SolarEdge API.
*   `peewee`: An ORM for interacting with the PostgreSQL database.
*   `dynaconf`: For managing application configuration.
*   `psycopg2-binary`: PostgreSQL adapter for Python.
*   `demjson3` (optional, used by `json_parser.py`): For parsing non-standard JSON.

## Setup and Usage

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
2.  **Configure `config/settings.toml`**:
    *   Update the `[postgres]` section with your PostgreSQL database credentials.
    *   Review and update the `[api]` section if necessary, especially the `cookie` in `[api.headers]` if API authentication changes.
3.  **Database Setup**:
    *   Ensure your PostgreSQL server is running and the specified database and schema (`solar`) exist.
    *   The application will attempt to create the tables (`solar_installations`, `site_production_data`) if they don't exist within the `solar` schema.
4.  **Run the Scripts**:
    *   To import site metadata:
        ```bash
        python import_solaredge_sites.py
        ```
    *   To download CSV production data for imported sites:
        ```bash
        python download_site_csvs.py
        ```
    *   To upload production data from downloaded CSVs into the database:
        ```bash
        python upload_production_data.py
        ```
    *   To calculate and store normalized yearly generation profiles:
        ```bash
        python calculate_yearly_profiles.py
        ```

## Workflow

1.  Run `import_solaredge_sites.py` to populate the `solar_installations` table with site details from the API.
2.  Run `download_site_csvs.py` to fetch historical production data as CSV files for these sites. This script will mark sites as `has_csv=True`.
3.  Run `upload_production_data.py` to parse these CSVs and load the time-series production data into the `site_production_data` table, and mark sites with `uploaded_on` timestamp.
4.  Run `calculate_yearly_profiles.py` to process the uploaded production data, calculate a normalized 15-minute interval generation profile for a representative year for each eligible site, and store it in the `site_reference_year_production` table. This script also updates `profile_updated_on` for the site.

This structured approach allows for modular data handling, from initial site discovery to detailed production data storage and yearly profile generation.
