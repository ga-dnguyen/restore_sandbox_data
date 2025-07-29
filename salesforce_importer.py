import os
import pandas as pd
from dotenv import load_dotenv
from simple_salesforce import Salesforce
import argparse
import json

def get_salesforce_connection():
    """Establishes and returns a Salesforce connection."""
    load_dotenv(override=True)
    sf_username = os.getenv("SALESFORCE_USERNAME")
    sf_password = os.getenv("SALESFORCE_PASSWORD")
    sf_consumer_key = os.getenv("SALESFORCE_CONSUMER_KEY")
    sf_consumer_secret = os.getenv("SALESFORCE_CONSUMER_SECRET")
    sf_domain = os.getenv("SALESFORCE_DOMAIN", "login")
    print(f"Connecting to Salesforce with username: {sf_username}")
    print(f"Connecting to Salesforce with password: {sf_password}")
    print(f"Connecting to Salesforce with consumer key: {sf_consumer_key}")
    print(f"Connecting to Salesforce with consumer secret: {sf_consumer_secret}")
    print(f"Connecting to Salesforce with domain: {sf_domain}")

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
        # Collect all non-createable and all formula (calculated) fields
        readonly = {field['name'] for field in sobject_desc['fields'] if not field['createable'] or field['calculated']}
        # For clarity, also collect all formula fields (calculated=True)
        formula_fields = {field['name'] for field in sobject_desc['fields'] if field['calculated']}
        # Keep 'IsPersonAccount' for logic, as it's needed to identify person accounts.
        readonly.discard('IsPersonAccount')
        # Add all formula fields to readonly set to ensure they are dropped
        readonly = readonly.union(formula_fields)
        return readonly
    except Exception as e:
        print(f"Could not describe object {object_name}: {e}")
        return set()

def load_default_records():
    """Load default records from default_records.json file."""
    try:
        with open('default_records.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("default_records.json file not found. Skipping default record creation.")
        return {}
    except Exception as e:
        print(f"Error loading default_records.json: {e}")
        return {}

def load_lookup_field_mappings():
    """Load lookup field mappings from lookup_field_mappings.json file."""
    try:
        with open('lookup_field_mappings.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("lookup_field_mappings.json file not found. Lookup field replacement disabled.")
        return {}
    except Exception as e:
        print(f"Error loading lookup_field_mappings.json: {e}")
        return {}

def replace_lookup_fields_with_defaults(sf, obj_name, insert_df, default_record_ids, lookup_mappings):
    """Replace lookup field values with default record IDs where applicable."""
    if obj_name not in lookup_mappings:
        return insert_df
    
    modified_df = insert_df.copy()
    object_lookup_fields = lookup_mappings[obj_name]
    
    for field_name, field_info in object_lookup_fields.items():
        if field_name not in modified_df.columns:
            continue
            
        # Skip non-createable fields
        if not field_info.get('createable', False):
            continue
            
        referenced_objects = field_info.get('referenceTo', [])
        
        # Find matching default record for this lookup field
        for ref_object in referenced_objects:
            if ref_object in default_record_ids:
                # Only replace non-blank values (not NaN, not None, not empty string)
                mask = modified_df[field_name].notna() & (modified_df[field_name] != '') & (modified_df[field_name] != ' ')
                original_count = mask.sum()
                if original_count > 0:
                    modified_df.loc[mask, field_name] = default_record_ids[ref_object]
                    print(f"  Replaced {original_count} non-blank {field_name} values with default {ref_object} ID: {default_record_ids[ref_object]}")
                break
    
    return modified_df

def create_default_records(sf, default_records):
    """Create default records in Salesforce before importing data."""
    print("--- Creating Default Records ---")
    default_record_ids = {}
    
    # Define creation order to handle dependencies
    creation_order = ['Account', 'Lead', 'Apart__c', 'Opportunity', 'Room__c', 'Buyer__c', 'Transcript__c']
    
    for obj_name in creation_order:
        if obj_name not in default_records:
            continue
            
        record_data = default_records[obj_name].copy()  # Make a copy to avoid modifying original
        
        # Set foreign key relationships
        if obj_name == 'Opportunity' and 'Account' in default_record_ids:
            record_data['AccountId'] = default_record_ids['Account']
            print(f"  Setting AccountId to default Account: {default_record_ids['Account']}")
        
        if obj_name == 'Room__c' and 'Apart__c' in default_record_ids:
            record_data['RoomApart__c'] = default_record_ids['Apart__c']
            print(f"  Setting RoomApart__c to default Apart: {default_record_ids['Apart__c']}")
        
        print(f"Creating default record for {obj_name}...")
        try:
            headers = {'Sforce-Duplicate-Rule-Header': 'allowSave=true'}
            result = sf.restful(f'sobjects/{obj_name}/', method='POST', json=record_data, headers=headers)
            if result.get('success'):
                default_record_ids[obj_name] = result.get('id')
                print(f"  Successfully created default {obj_name} record: {result.get('id')}")
            else:
                print(f"  Failed to create default {obj_name} record: {result}")
        except Exception as e:
            print(f"  Error creating default {obj_name} record: {e}")
    
    return default_record_ids

def main():
    """Main function to handle the data import process."""
    sf = get_salesforce_connection()
    if not sf:
        print("Failed to connect to Salesforce. Exiting.")
        return

    print("Successfully connected to Salesforce for import.")

    # Load and create default records
    default_records = load_default_records()
    default_record_ids = {}
    if default_records:
        default_record_ids = create_default_records(sf, default_records)

    # Load lookup field mappings for replacement
    lookup_mappings = load_lookup_field_mappings()

    # --- Define Import Order (Parent objects first) ---
    # This order is critical. Adjust if you have different dependencies.
    import_order = [
        'Account',
        'Lead',
        'Opportunity',
        'Apart__c',
        'Room__c',
        'Buyer__c',
        'Transcript__c',
        'MP_Action__c',
        'OpportunityLog__c',
        'ValuationLog__c'
    ]

    parser = argparse.ArgumentParser(description='Import Salesforce data from CSV files.')
    parser.add_argument('--object', type=str, help='The specific Salesforce object to import (e.g., Account). If not provided, all objects will be imported.')
    args = parser.parse_args()

    data_dir = "exported_data"
    objects_to_process = []

    if args.object:
        # If a specific object is provided, only process that one.
        if args.object in import_order:
            objects_to_process = [args.object]
        else:
            print(f"Error: Object '{args.object}' is not in the defined import_order list.")
            return
    else:
        # Otherwise, process all objects defined in the import order.
        objects_to_process = import_order

    id_map = {}

    for obj_name in objects_to_process:
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

        # Replace lookup fields with default record IDs
        if default_record_ids and lookup_mappings:
            print(f"  Checking lookup fields for default record replacement...")
            insert_df = replace_lookup_fields_with_defaults(sf, obj_name, insert_df, default_record_ids, lookup_mappings)

        # Convert DataFrame to a list of dictionaries
        records_to_insert = insert_df.to_dict('records')

        # Clean records: replace NaN with None and handle Person Accounts
        cleaned_records = []
        for record in records_to_insert:
            # Remove all keys with None or NaN values
            cleaned_record = {k: v for k, v in record.items() if v is not None and not (isinstance(v, float) and pd.isna(v))}

            # Special handling for 'room__c' on Account
            if obj_name == 'Account' and 'room__c' in cleaned_record:
                del cleaned_record['room__c']

            # Special handling for 'NewDmOwnerId__c' on Lead
            if obj_name == 'Lead' and 'NewDmOwnerId__c' in cleaned_record:
                del cleaned_record['NewDmOwnerId__c']

            # Remove ConvertedDate from Leads to prevent FIELD_INTEGRITY_EXCEPTION
            if obj_name == 'Lead' and 'ConvertedDate' in cleaned_record:
                del cleaned_record['ConvertedDate']

            # If it's a Person Account, 'Name' is read-only and must be removed.
            if cleaned_record.get('IsPersonAccount'):
                if 'Name' in cleaned_record:
                    del cleaned_record['Name']

            # Remove 'IsPersonAccount' from the final payload as it's not writeable.
            if 'IsPersonAccount' in cleaned_record:
                del cleaned_record['IsPersonAccount']

            # Special handling for MP_Action__c: always set LastModifiedById
            if obj_name == 'MP_Action__c':
                cleaned_record['LastModifiedById'] = '0052j000000kxjEAAQ'

            cleaned_records.append(cleaned_record)
        records_to_insert = cleaned_records

        # Insert records using bulk API for better performance
        try:
            print(f"  Starting bulk insert for {len(records_to_insert)} records...")
            
            if len(records_to_insert) == 0:
                print(f"  No records to insert for {obj_name}.")
                continue
            
            # Use bulk API for better performance
            try:
                # Use the bulk upsert method which is more reliable
                bulk_results = sf.bulk.__getattr__(obj_name).insert(records_to_insert)
                
                successful_inserts = 0
                new_ids = []
                failed_records = []
                
                # Process bulk results
                for i, result in enumerate(bulk_results):
                    if result.get('success') == True or result.get('success') == 'true':
                        new_ids.append(result.get('id'))
                        successful_inserts += 1
                    else:
                        new_ids.append(None)
                        # Collect detailed error information
                        error_info = {
                            'record_index': i + 1,
                            'record_data': records_to_insert[i] if i < len(records_to_insert) else {},
                            'errors': []
                        }
                        
                        # Extract error details from different possible formats
                        if 'error' in result:
                            error_info['errors'].append(result['error'])
                        
                        if 'errors' in result:
                            if isinstance(result['errors'], list):
                                for error in result['errors']:
                                    if isinstance(error, dict):
                                        error_msg = error.get('message', str(error))
                                        error_code = error.get('statusCode', '')
                                        error_fields = error.get('fields', [])
                                        full_error = f"{error_code}: {error_msg}"
                                        if error_fields:
                                            full_error += f" (Fields: {', '.join(error_fields)})"
                                        error_info['errors'].append(full_error)
                                    else:
                                        error_info['errors'].append(str(error))
                            else:
                                error_info['errors'].append(str(result['errors']))
                        
                        # If no specific errors found, add a generic message
                        if not error_info['errors']:
                            error_info['errors'].append(f"Unknown error - Result: {result}")
                        
                        failed_records.append(error_info)
                
                print(f"    Bulk operation completed: {successful_inserts} successful, {len(bulk_results) - successful_inserts} failed")
                
                # Display detailed error information for failed records
                if failed_records:
                    print(f"    *** FAILED RECORD DETAILS ***")
                    for error_info in failed_records[:10]:  # Show first 10 failures to avoid spam
                        print(f"    Record {error_info['record_index']}:")
                        for error in error_info['errors']:
                            print(f"      Error: {error}")
                        # Show a few key fields from the failed record for context
                        record_data = error_info['record_data']
                        key_fields = ['Name', 'LastName', 'FirstName', 'Email', 'Company']
                        context_fields = {k: v for k, v in record_data.items() if k in key_fields and v}
                        if context_fields:
                            print(f"      Record context: {context_fields}")
                        print()
                    
                    if len(failed_records) > 10:
                        print(f"    ... and {len(failed_records) - 10} more failed records")
                
            except Exception as bulk_error:
                print(f"    Bulk API error: {bulk_error}")
                raise bulk_error  # Re-raise to trigger fallback

            # Filter out None values from new_ids and corresponding original_ids before mapping
            valid_original_ids = [old_id for old_id, new_id in zip(original_ids, new_ids) if new_id is not None]
            valid_new_ids = [new_id for new_id in new_ids if new_id is not None]

            if valid_new_ids:
                id_map[f"{obj_name}Id"] = dict(zip(valid_original_ids, valid_new_ids))
                print(f"  Successfully inserted {successful_inserts} of {len(records_to_insert)} records for {obj_name}.")
            else:
                print(f"  No records were successfully inserted for {obj_name}.")

        except Exception as e:
            print(f"An error occurred during bulk insert for {obj_name}: {e}")
            print("Falling back to single record insert...")
            
            # Fallback to single record insert if bulk fails
            new_ids = []
            successful_inserts = 0
            for i, record in enumerate(records_to_insert):
                try:
                    headers = {'Sforce-Duplicate-Rule-Header': 'allowSave=true'}
                    result = sf.restful(f'sobjects/{obj_name}/', method='POST', json=record, headers=headers)
                    if result.get('success'):
                        new_ids.append(result.get('id'))
                        successful_inserts += 1
                        if (i + 1) % 100 == 0:  # Progress indicator every 100 records
                            print(f"    Processed {i + 1}/{len(records_to_insert)} records...")
                    else:
                        new_ids.append(None)
                        error_details = result.get('errors', [])
                        if error_details:
                            print(f"    Record {i+1} failed: {error_details[0].get('message', 'Unknown error')}")
                except Exception as record_error:
                    new_ids.append(None)
                    print(f"    Record {i+1} exception: {record_error}")

            # Filter out None values for fallback method
            valid_original_ids = [old_id for old_id, new_id in zip(original_ids, new_ids) if new_id is not None]
            valid_new_ids = [new_id for new_id in new_ids if new_id is not None]

            if valid_new_ids:
                id_map[f"{obj_name}Id"] = dict(zip(valid_original_ids, valid_new_ids))
                print(f"  Successfully inserted {successful_inserts} of {len(records_to_insert)} records for {obj_name} (fallback method).")
            else:
                print(f"  No records were successfully inserted for {obj_name}.")

if __name__ == "__main__":
    main()
