
from domain.sites_importer_service import import_solar_data

if __name__ == "__main__":
    # You can set a limit for testing, or None to fetch all
    import_solar_data(max_total_records_to_fetch=None)
