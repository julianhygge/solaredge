import peewee
from peewee import Model
from config.loader import settings
from peewee import Model, PostgresqlDatabase
import peewee

pg_conf = settings.postgres

db = PostgresqlDatabase(
    pg_conf.database,
    user=pg_conf.user,
    password=pg_conf.password,
    host=pg_conf.host,
    port=pg_conf.port,
    autorollback=True,
    options='-c search_path=solar'
)

class BaseModel(Model):
    class Meta:
        database = db
        schema = 'solar'

class SolarSite(BaseModel):
    site_id = peewee.IntegerField(unique=True, primary_key=True)
    name = peewee.CharField(null=True)
    status = peewee.CharField(null=True)
    peak_power = peewee.CharField(null=True)
    type = peewee.CharField(null=True)
    zip_code = peewee.CharField(null=True)
    address = peewee.CharField(null=True)
    country = peewee.CharField(null=True)
    state = peewee.CharField(null=True)
    city = peewee.CharField(null=True)
    location = peewee.CharField(null=True)
    secondary_address = peewee.CharField(null=True)
    installation_date = peewee.CharField(null=True)
    last_reporting_time = peewee.CharField(null=True)
    updated_on = peewee.DateTimeField(null=True)
    uploaded_on = peewee.DateTimeField(null=True) # New field
    has_csv = peewee.BooleanField(default=False)

    class Meta:
        table_name = 'solar_installations'
        schema = 'solar'

# Ensure schema and table exist
if not db.table_exists('solar_installations', schema='solar'):
    db.create_tables([SolarSite], safe=True)

class SiteProductionData(BaseModel):
    site = peewee.ForeignKeyField(SolarSite, backref='production_data', field=SolarSite.site_id)
    timestamp = peewee.DateTimeField()
    production = peewee.IntegerField()

    class Meta:
        table_name = 'site_production_data'
        schema = 'solar'
        # Add a composite unique constraint to prevent duplicate entries for the same site and timestamp
        indexes = (
            (('site', 'timestamp'), True),
        )

# Ensure the new table exists
if not db.table_exists('site_production_data', schema='solar'):
    db.create_tables([SiteProductionData], safe=True)
