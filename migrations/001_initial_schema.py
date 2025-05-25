import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import toml
import os
from pathlib import Path

def load_config():
    """Load database configuration from settings.toml"""
    config_path = Path(__file__).parent.parent / 'config' / 'settings.toml'
    with open(config_path) as f:
        config = toml.load(f)
    return config['postgres']

def get_connection(config):
    """Create a database connection"""
    return psycopg2.connect(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database=config['database']
    )

def schema_exists(conn, schema_name):
    """Check if schema exists"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
            (schema_name,)
        )
        return cur.fetchone() is not None

def create_schema(conn, schema_name):
    """Create schema if it doesn't exist"""
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(schema_name)
        ))
    conn.commit()
    print(f"Schema '{schema_name}' created successfully")

def table_exists(conn, schema_name, table_name):
    """Check if table exists in the schema"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        """, (schema_name, table_name))
        return cur.fetchone() is not None

def get_table_columns(conn, schema_name, table_name):
    """Get column information for a table"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema_name, table_name))
        return {row[0]: row for row in cur.fetchall()}

def create_table(conn, schema_name, table_name):
    """Create the solar_installations table with the specified schema"""
    create_table_sql = f"""
    CREATE TABLE {schema_name}.{table_name} (
        site_id integer NOT NULL,
        name character varying(255) COLLATE pg_catalog."default",
        status character varying(255) COLLATE pg_catalog."default",
        peak_power character varying(255) COLLATE pg_catalog."default",
        type character varying(255) COLLATE pg_catalog."default",
        zip_code character varying(255) COLLATE pg_catalog."default",
        address character varying(255) COLLATE pg_catalog."default",
        country character varying(255) COLLATE pg_catalog."default",
        state character varying(255) COLLATE pg_catalog."default",
        city character varying(255) COLLATE pg_catalog."default",
        installation_date character varying(255) COLLATE pg_catalog."default",
        last_reporting_time character varying(255) COLLATE pg_catalog."default",
        location character varying(255) COLLATE pg_catalog."default",
        secondary_address character varying(255) COLLATE pg_catalog."default",
        updated_on timestamp without time zone,
        has_csv boolean NOT NULL DEFAULT false,
        uploaded_on timestamp without time zone,
        profile_updated_on timestamp without time zone,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (site_id)
    )
    """
    
    with conn.cursor() as cur:
        cur.execute(sql.SQL(create_table_sql).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name)
        ))
    conn.commit()
    print(f"Table '{schema_name}.{table_name}' created successfully")

def update_table_schema(conn, schema_name, table_name, existing_columns):
    """Update table schema if needed"""
    # Define the expected columns and their definitions
    expected_columns = {
        'site_id': {'data_type': 'integer', 'is_nullable': 'NO', 'column_default': None},
        'name': {'data_type': 'character varying', 'character_maximum_length': 255, 'is_nullable': 'YES', 'column_default': None},
        'status': {'data_type': 'character varying', 'character_maximum_length': 255, 'is_nullable': 'YES', 'column_default': None},
        'peak_power': {'data_type': 'character varying', 'character_maximum_length': 255, 'is_nullable': 'YES', 'column_default': None},
        'type': {'data_type': 'character varying', 'character_maximum_length': 255, 'is_nullable': 'YES', 'column_default': None},
        'zip_code': {'data_type': 'character varying', 'character_maximum_length': 255, 'is_nullable': 'YES', 'column_default': None},
        'address': {'data_type': 'character varying', 'character_maximum_length': 255, 'is_nullable': 'YES', 'column_default': None},
        'country': {'data_type': 'character varying', 'character_maximum_length': 255, 'is_nullable': 'YES', 'column_default': None},
        'state': {'data_type': 'character varying', 'character_maximum_length': 255, 'is_nullable': 'YES', 'column_default': None},
        'city': {'data_type': 'character varying', 'character_maximum_length': 255, 'is_nullable': 'YES', 'column_default': None},
        'installation_date': {'data_type': 'character varying', 'character_maximum_length': 255, 'is_nullable': 'YES', 'column_default': None},
        'last_reporting_time': {'data_type': 'character varying', 'character_maximum_length': 255, 'is_nullable': 'YES', 'column_default': None},
        'location': {'data_type': 'character varying', 'character_maximum_length': 255, 'is_nullable': 'YES', 'column_default': None},
        'secondary_address': {'data_type': 'character varying', 'character_maximum_length': 255, 'is_nullable': 'YES', 'column_default': None},
        'updated_on': {'data_type': 'timestamp without time zone', 'is_nullable': 'YES', 'column_default': None},
        'has_csv': {'data_type': 'boolean', 'is_nullable': 'NO', 'column_default': 'false'},
        'uploaded_on': {'data_type': 'timestamp without time zone', 'is_nullable': 'YES', 'column_default': None},
        'profile_updated_on': {'data_type': 'timestamp without time zone', 'is_nullable': 'YES', 'column_default': None}
    }
    
    with conn.cursor() as cur:
        # Check for missing columns
        for col_name, col_def in expected_columns.items():
            if col_name not in existing_columns:
                # Column is missing, add it
                col_type = col_def['data_type']
                if 'character_maximum_length' in col_def and col_def['character_maximum_length']:
                    col_type += f"({col_def['character_maximum_length']})"
                
                add_col_sql = f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {col_name} {col_type}"
                if col_def['is_nullable'] == 'NO':
                    add_col_sql += " NOT NULL"
                if col_def['column_default'] is not None:
                    add_col_sql += f" DEFAULT {col_def['column_default']}"
                
                print(f"Adding column {col_name} to {schema_name}.{table_name}")
                cur.execute(sql.SQL(add_col_sql))
        
        # Check for primary key
        cur.execute("""
            SELECT 1 
            FROM information_schema.table_constraints 
            WHERE table_schema = %s 
              AND table_name = %s 
              AND constraint_type = 'PRIMARY KEY'
        """, (schema_name, table_name))
        
        if not cur.fetchone():
            # Add primary key if it doesn't exist
            add_pk_sql = f"""
            ALTER TABLE {0}.{1} 
            ADD CONSTRAINT {1}_pkey 
            PRIMARY KEY (site_id)
            """.format(schema_name, table_name)
            
            print(f"Adding primary key to {schema_name}.{table_name}")
            cur.execute(sql.SQL(add_pk_sql))
        
        conn.commit()

def main():
    try:
        # Load configuration
        config = load_config()
        schema_name = config['schema']
        table_name = config['table']
        
        # Connect to PostgreSQL
        conn = get_connection(config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        try:
            # Check and create schema if needed
            if not schema_exists(conn, schema_name):
                print(f"Schema '{schema_name}' does not exist. Creating...")
                create_schema(conn, schema_name)
            
            # Check if table exists
            if not table_exists(conn, schema_name, table_name):
                print(f"Table '{schema_name}.{table_name}' does not exist. Creating...")
                create_table(conn, schema_name, table_name)
            else:
                print(f"Table '{schema_name}.{table_name}' already exists. Checking schema...")
                # Get existing columns
                existing_columns = get_table_columns(conn, schema_name, table_name)
                # Update schema if needed
                update_table_schema(conn, schema_name, table_name, existing_columns)
                print(f"Table '{schema_name}.{table_name}' schema verified and updated if needed")
            
            print("Database setup completed successfully")
            
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
