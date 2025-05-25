from .models import SolarSite, db

class SolarSiteRepository:
    def __init__(self):
        db.connect(reuse_if_open=True)
        db.create_tables([SolarSite], safe=True)

    def add_or_update(self, site_data):
        return SolarSite.insert(site_data).on_conflict(
            conflict_target=[SolarSite.site_id],
            update={
                SolarSite.name: site_data['name'],
                SolarSite.status: site_data['status'],
                SolarSite.peak_power: site_data['peak_power'],
                SolarSite.type: site_data['type'],
                SolarSite.zip_code: site_data['zip_code'],
                SolarSite.address: site_data['address'],
                SolarSite.country: site_data['country'],
                SolarSite.state: site_data['state'],
                SolarSite.city: site_data['city'],
                SolarSite.installation_date: site_data['installation_date'],
                SolarSite.last_reporting_time: site_data['last_reporting_time'],
            }
        ).execute()

    def get_all(self):
        return list(SolarSite.select())

    def get_by_id(self, site_id):
        return SolarSite.get_or_none(SolarSite.site_id == site_id)
