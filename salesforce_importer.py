import os
import pandas as pd
from dotenv import load_dotenv
from simple_salesforce import Salesforce

def get_salesforce_connection():
    """Establishes and returns a Salesforce connection."""
    load_dotenv()
    sf_username = os.getenv("SALESFORCE_USERNAME")
    sf_password = os.getenv("SALESFORCE_PASSWORD")
    sf_consumer_key = os.getenv("SALESFORCE_CONSUMER_KEY")
    sf_consumer_secret = os.getenv("SALESFORCE_CONSUMER_SECRET")
    sf_domain = os.getenv("SALESFORCE_DOMAIN", "login")

    sf = Salesforce(
        username=sf_username,
        password=sf_password,
        consumer_key=sf_consumer_key,
        consumer_secret=sf_consumer_secret,
        domain=sf_domain
    )
    return sf

def get_readonly_fields(sf, object_name):
    """Gets a list of read-only fields for an object that cannot be set on insert."""
    try:
        sobject_desc = getattr(sf, object_name).describe()
        # Fields that are not createable or are system-generated
        return {field['name'] for field in sobject_desc['fields'] if not field['createable'] or field['calculated']}
    except Exception as e:
        print(f"Could not describe object {object_name}: {e}")
        return set()

def main():
    """Main function to handle the data import process."""
    sf = get_salesforce_connection()
    if not sf:
        print("Failed to connect to Salesforce. Exiting.")
        return

    print("Successfully connected to Salesforce for import.")

    # --- Define Import Order (Parent objects first) ---
    # This order is critical. Adjust if you have different dependencies.
    import_order = [
        'Account',
        'Lead',
        'Opportunity',
        'Apart__c',
        'Room__c',
        'Buyer__c',
        'MP_Action__c',
        'OpportunityLog__c',
        'ValuationLog__c'
    ]

    data_dir = "exported_data"
    id_map = {}

    for obj_name in import_order:
        csv_path = os.path.join(data_dir, f"{obj_name}.csv")
        if not os.path.exists(csv_path):
            print(f"CSV file not found for {obj_name}, skipping.")
            continue

        print(f"--- Processing {obj_name} --- ")
        df = pd.read_csv(csv_path)

        if 'Id' not in df.columns:
            print(f"'Id' column not found in {csv_path}, skipping.")
            continue

        # Store original IDs
        original_ids = df['Id'].tolist()

        # Clean data for insertion
        readonly_fields = get_readonly_fields(sf, obj_name)
        # Also remove the original Id field itself
        fields_to_drop = list(readonly_fields.intersection(df.columns)) + ['Id']
        insert_df = df.drop(columns=fields_to_drop, errors='ignore')

        # Replace foreign keys with new IDs from the map
        for col in insert_df.columns:
            if col.endswith('Id') and col in id_map:
                print(f"  Mapping foreign key column: {col}")
                insert_df[col] = insert_df[col].map(id_map[col])

        # Replace NaN with None for Salesforce compatibility
        insert_df = insert_df.where(pd.notnull(insert_df), None)

        # Convert to list of dicts for API
        records_to_insert = insert_df.to_dict('records')

        # Insert records one by one using the standard REST API
        try:
            print(f"  Starting insert for {len(records_to_insert)} records...")
            new_ids = []
            successful_inserts = 0
            for record in records_to_insert:
                print(record)
                try:
                    result = getattr(sf, obj_name).create(record)
                    if result.get('success'):
                        new_ids.append(result.get('id'))
                        # Append None to keep lists aligned for zipping
                        new_ids.append(None)
                        print(f"    Failed to insert record. Errors: {result.get('errors')}")
                except Exception as record_error:
                    new_ids.append(None)
                    print(f"    An exception occurred while inserting a record: {record_error}")

            # Filter out None values from new_ids and corresponding original_ids before mapping
            valid_original_ids = [old_id for old_id, new_id in zip(original_ids, new_ids) if new_id is not None]
            valid_new_ids = [new_id for new_id in new_ids if new_id is not None]

            if valid_new_ids:
                id_map[f"{obj_name}Id"] = dict(zip(valid_original_ids, valid_new_ids))
                print(f"  Successfully inserted {successful_inserts} of {len(records_to_insert)} records for {obj_name}.")
            else:
                print(f"  No records were successfully inserted for {obj_name}.")

        except Exception as e:
            print(f"An error occurred during insert for {obj_name}: {e}")

if __name__ == "__main__":
    main()
