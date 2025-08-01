import os
import pandas as pd
from dotenv import load_dotenv
from simple_salesforce import Salesforce
import argparse
import json
import logging
import glob
from datetime import datetime
from objects_config import OBJECTS_LIST

# Global cache for Salesforce object descriptions to avoid repeated API calls
_sf_describe_cache = {}

def get_sobject_description(sf, object_name):
    """Get Salesforce object description with caching to avoid repeated API calls."""
    global _sf_describe_cache
    
    if object_name not in _sf_describe_cache:
        try:
            _sf_describe_cache[object_name] = getattr(sf, object_name).describe()
        except Exception as e:
            print(f"Error describing {object_name}: {e}")
            return None
    
    return _sf_describe_cache[object_name]

def clear_describe_cache():
    """Clear the object description cache. Useful when connecting to a different org."""
    global _sf_describe_cache
    _sf_describe_cache = {}

def setup_logging():
    """Set up logging to file with timestamp."""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Generate timestamp for log filename (YYYYMMDD_HHSS format)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    log_filename = os.path.join('logs', f'import_log_{timestamp}.log')
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()  # Also log to console
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Import session started - Log file: {log_filename}")
    return logger

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

def get_lookup_relationships(sf, object_name):
    """Get all lookup relationships for an object"""
    sobject_desc = get_sobject_description(sf, object_name)
    if not sobject_desc:
        return {}
    
    lookup_fields = {}
    
    for field in sobject_desc['fields']:
        if field['type'] == 'reference':
            field_name = field['name']
            referenced_objects = field.get('referenceTo', [])
            if referenced_objects:  # Only include fields that reference other objects
                lookup_fields[field_name] = {
                    'label': field['label'],
                    'referenceTo': referenced_objects,
                    'createable': field['createable'],
                    'updateable': field['updateable']
                }
    
    return lookup_fields

def generate_lookup_field_mappings(sf, objects_to_process):
    """Generate lookup field mappings for the objects being processed."""
    print("--- Generating Lookup Field Mappings ---")
    all_lookup_mappings = {}
    
    for obj_name in objects_to_process:
        print(f"  Analyzing {obj_name}...")
        lookup_fields = get_lookup_relationships(sf, obj_name)
        
        if lookup_fields:
            all_lookup_mappings[obj_name] = lookup_fields
            print(f"    Found {len(lookup_fields)} lookup fields")
        else:
            print(f"    No lookup fields found")
    
    # Save to JSON file for reference
    with open('lookup_field_mappings.json', 'w') as f:
        json.dump(all_lookup_mappings, f, indent=2)
    
    print(f"  Saved lookup field mappings to lookup_field_mappings.json")
    return all_lookup_mappings

def get_readonly_fields(sf, object_name):
    """Gets a list of read-only fields for an object that cannot be set on insert."""
    sobject_desc = get_sobject_description(sf, object_name)
    if not sobject_desc:
        return set()
    
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

def get_available_fields(sf, object_name):
    """Gets a set of all available fields for an object in the current org."""
    sobject_desc = get_sobject_description(sf, object_name)
    if not sobject_desc:
        return set()
    
    return {field['name'] for field in sobject_desc['fields']}

def validate_and_replace_user_ids(sf, obj_name, insert_df, default_user_id='005BL000000IBL8YAO'):
    """Validate User IDs and replace non-existent ones with default User ID."""
    # Get object description to find all user lookup fields
    sobject_desc = get_sobject_description(sf, obj_name)
    if not sobject_desc:
        print(f"  Warning: Could not get object description for {obj_name}, skipping User ID validation")
        return insert_df
    
    # Find all fields that reference the User object
    user_fields = []
    for field in sobject_desc['fields']:
        if field['type'] == 'reference' and 'User' in field.get('referenceTo', []):
            user_fields.append(field['name'])
    
    if not user_fields:
        print(f"  No User lookup fields found for {obj_name}")
        return insert_df
    
    print(f"  Found {len(user_fields)} User lookup fields: {', '.join(user_fields)}")
    
    for field_name in user_fields:
        if field_name not in insert_df.columns:
            continue
            
        # Get all non-null User IDs for this field
        non_null_mask = insert_df[field_name].notna() & (insert_df[field_name] != '') & (insert_df[field_name] != ' ')
        if not non_null_mask.any():
            continue
            
        unique_user_ids = insert_df.loc[non_null_mask, field_name].unique()
        
        # Filter to only User IDs (starting with '005')
        user_ids_to_check = [uid for uid in unique_user_ids if isinstance(uid, str) and uid.startswith('005')]
        
        if not user_ids_to_check:
            continue
            
        try:
            # Check which User IDs exist in the org
            id_list = "','".join(user_ids_to_check)
            query = f"SELECT Id FROM User WHERE Id IN ('{id_list}') AND IsActive = true"
            results = sf.query(query)
            
            existing_user_ids = {record['Id'] for record in results['records']}
            missing_user_ids = [uid for uid in user_ids_to_check if uid not in existing_user_ids]
            
            if missing_user_ids:
                print(f"  Replacing {len(missing_user_ids)} non-existent User IDs in {field_name} with default: {default_user_id}")
                for missing_id in missing_user_ids:
                    mask = insert_df[field_name] == missing_id
                    insert_df.loc[mask, field_name] = default_user_id
                    
        except Exception as e:
            print(f"  Warning: Could not validate User IDs for {field_name}: {e}")
            # If validation fails, replace all User IDs with default to be safe
            print(f"  Replacing all {field_name} values with default User ID due to validation error")
            insert_df.loc[non_null_mask, field_name] = default_user_id
    
    return insert_df

def read_csv_with_string_fields_preserved(sf, obj_name, csv_path):
    """Read CSV file with text and phone fields treated as strings to prevent unwanted numeric conversion."""
    try:
        # First, identify text-based fields for this object
        sobject_desc = get_sobject_description(sf, obj_name)
        if not sobject_desc:
            print(f"  Could not get object description for {obj_name}, falling back to normal CSV read")
            return pd.read_csv(csv_path)
        
        # Get fields that should be treated as strings to prevent numeric conversion
        string_fields = []
        for field in sobject_desc['fields']:
            field_type = field['type']
            # Include phone, text, textarea, string, url, email, and picklist fields
            if field_type in ['phone', 'string', 'textarea', 'url', 'email', 'picklist', 'multipicklist', 'combobox']:
                string_fields.append(field['name'])
        
        # Create dtype dictionary to force string-based fields to be read as strings
        dtype_dict = {field: str for field in string_fields}
        
        # Read CSV with string-based fields as strings
        df = pd.read_csv(csv_path, dtype=dtype_dict)
        
        if string_fields:
            print(f"  Read CSV with {len(string_fields)} text-based fields as strings to preserve formatting")
        
        return df
        
    except Exception as e:
        print(f"  Could not read with field type detection, falling back to normal CSV read: {e}")
        # Fallback to normal CSV reading
        return pd.read_csv(csv_path)

def fix_text_field_formatting(sf, obj_name, insert_df):
    """Fix text and phone fields that may have been interpreted as scientific notation or unwanted float conversion."""
    try:
        # Get field descriptions to identify text-based fields
        sobject_desc = get_sobject_description(sf, obj_name)
        if not sobject_desc:
            print(f"Error getting object description for {obj_name}")
            return insert_df
        
        # Get fields that should be strings but might have been converted to numbers
        text_based_fields = []
        for field in sobject_desc['fields']:
            field_type = field['type']
            # Include phone, text, textarea, string, url, email, and picklist fields
            if field_type in ['phone', 'string', 'textarea', 'url', 'email', 'picklist', 'multipicklist', 'combobox']:
                text_based_fields.append(field['name'])
        
        modified_df = insert_df.copy()
        
        for field_name in text_based_fields:
            if field_name in modified_df.columns:
                # Convert any numeric values back to clean string format
                def fix_text_value(value):
                    if pd.isna(value) or value == '' or value == ' ':
                        return value
                    
                    # Convert to string first
                    str_value = str(value)
                    
                    # Check if it's in scientific notation (contains 'E' or 'e')
                    if 'E' in str_value.upper():
                        try:
                            # Convert scientific notation to integer, then to string
                            # This handles cases like 8.011111111E9 -> 8011111111
                            numeric_value = float(str_value)
                            # Only convert if it's a whole number (no decimal places after conversion)
                            if numeric_value == int(numeric_value):
                                return str(int(numeric_value))
                            else:
                                return str_value
                        except (ValueError, OverflowError):
                            return str_value
                    
                    # Check if it's a float that should be an integer (e.g., "10.0" -> "10")
                    if '.' in str_value and str_value.replace('.', '').replace('-', '').isdigit():
                        try:
                            float_val = float(str_value)
                            # If it's a whole number, convert to integer string
                            if float_val == int(float_val):
                                return str(int(float_val))
                        except (ValueError, OverflowError):
                            pass
                    
                    # For all other cases, ensure it's a string without .0 suffix
                    if str_value.endswith('.0') and str_value[:-2].replace('-', '').isdigit():
                        return str_value[:-2]
                    
                    return str_value
                
                # Apply the fix to all values in the text field
                modified_df[field_name] = modified_df[field_name].apply(fix_text_value)
                print(f"  Fixed text field formatting for field: {field_name}")
        
        return modified_df
        
    except Exception as e:
        print(f"Error fixing text field formatting for {obj_name}: {e}")
        return insert_df

def clean_lookup_references(sf, obj_name, insert_df, lookup_mappings):
    """Remove lookup field values that reference non-existent records."""
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
        
        # Get all non-null values for this field
        non_null_mask = modified_df[field_name].notna() & (modified_df[field_name] != '') & (modified_df[field_name] != ' ')
        if not non_null_mask.any():
            continue
            
        unique_ids = modified_df.loc[non_null_mask, field_name].unique()
        referenced_objects = field_info.get('referenceTo', [])
        
        # Special handling for Task object lookup fields using ID prefixes
        if obj_name == 'Task' and field_name in ['WhatId', 'WhoId']:
            print(f"  Validating {field_name} references using ID prefix detection...")
            
            # Group IDs by object type based on prefix
            ids_by_object_type = {}
            invalid_ids = []
            
            for unique_id in unique_ids:
                if not isinstance(unique_id, str) or len(unique_id) < 3:
                    invalid_ids.append(unique_id)
                    continue
                    
                # Check ID prefix to determine object type
                id_prefix = unique_id[:3]
                target_object = None
                
                if id_prefix == '001':  # Account
                    target_object = 'Account'
                elif id_prefix == '006':  # Opportunity
                    target_object = 'Opportunity'
                elif id_prefix == '00Q':  # Lead
                    target_object = 'Lead'
                
                if target_object and target_object in referenced_objects:
                    if target_object not in ids_by_object_type:
                        ids_by_object_type[target_object] = []
                    ids_by_object_type[target_object].append(unique_id)
                else:
                    invalid_ids.append(unique_id)
            
            # Clear invalid IDs (unknown prefixes or unsupported object types)
            if invalid_ids:
                print(f"    Clearing {len(invalid_ids)} {field_name} values with unsupported/invalid ID prefixes")
                for invalid_id in invalid_ids:
                    mask = modified_df[field_name] == invalid_id
                    modified_df.loc[mask, field_name] = None
            
            # Validate IDs by object type
            for target_object, ids_to_check in ids_by_object_type.items():
                try:
                    # Test a sample of IDs to see if they exist
                    test_ids = list(ids_to_check[:5])  # Test first 5 IDs
                    id_list = "','".join(test_ids)
                    query = f"SELECT Id FROM {target_object} WHERE Id IN ('{id_list}')"
                    results = sf.query(query)
                    
                    existing_ids = {record['Id'] for record in results['records']}
                    missing_count = len([id for id in test_ids if id not in existing_ids])
                    
                    if missing_count > 0:
                        print(f"    Warning: {missing_count}/{len(test_ids)} sampled {field_name} references to {target_object} don't exist")
                        if missing_count == len(test_ids):
                            # If all sampled IDs are missing, clear all IDs for this object type
                            print(f"    Clearing all {field_name} values referencing {target_object} (all sampled references missing)")
                            for missing_id in ids_to_check:
                                mask = modified_df[field_name] == missing_id
                                modified_df.loc[mask, field_name] = None
                        else:
                            # If only some are missing, keep the field values and let Salesforce handle validation
                            print(f"    Keeping {field_name} values referencing {target_object} (some references exist)")
                    
                except Exception as e:
                    print(f"    Could not validate {field_name} references to {target_object}: {e}")
                    # If we can't validate, clear all IDs for this object type to be safe
                    print(f"    Clearing all {field_name} values referencing {target_object} due to validation error")
                    for error_id in ids_to_check:
                        mask = modified_df[field_name] == error_id
                        modified_df.loc[mask, field_name] = None
            
            continue  # Skip the default logic for Task WhatId/WhoId fields
        
        # Default behavior for all other objects and fields
        # Check if referenced records exist for each referenced object type
        for ref_object in referenced_objects:
            try:
                # Try to query a few of the referenced IDs to see if they exist
                test_ids = list(unique_ids[:5])  # Test first 5 IDs
                id_list = "','".join(test_ids)
                query = f"SELECT Id FROM {ref_object} WHERE Id IN ('{id_list}')"
                results = sf.query(query)
                
                existing_ids = {record['Id'] for record in results['records']}
                missing_count = len([id for id in test_ids if id not in existing_ids])
                
                if missing_count > 0:
                    print(f"  Warning: {missing_count}/{len(test_ids)} sampled {field_name} references to {ref_object} don't exist")
                    if missing_count == len(test_ids):
                        # If all sampled IDs are missing, clear the entire field
                        print(f"    Clearing all {field_name} values (all sampled references missing)")
                        modified_df[field_name] = None
                    else:
                        # If only some are missing, we could try to validate each one, but that's expensive
                        # For now, just warn and let Salesforce handle the validation
                        print(f"    Keeping {field_name} values (some references exist)")
                
            except Exception as e:
                print(f"  Could not validate {field_name} references to {ref_object}: {e}")
                # If we can't validate, clear the field to be safe
                print(f"    Clearing {field_name} values due to validation error")
                modified_df[field_name] = None
                break
    
    return modified_df

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
        
        # Special handling for Task object lookup fields using ID prefixes
        if obj_name == 'Task' and field_name in ['WhatId', 'WhoId']:
            # Get all non-null values for this field
            non_null_mask = modified_df[field_name].notna() & (modified_df[field_name] != '') & (modified_df[field_name] != ' ')
            if not non_null_mask.any():
                continue
                
            # Process each non-null value to determine object type from ID prefix
            replacement_count = 0
            
            for idx in modified_df[non_null_mask].index:
                original_id = modified_df.loc[idx, field_name]
                if not isinstance(original_id, str) or len(original_id) < 3:
                    continue
                    
                # Check ID prefix to determine object type
                id_prefix = original_id[:3]
                target_object = None
                
                if id_prefix == '001':  # Account
                    target_object = 'Account'
                elif id_prefix == '006':  # Opportunity
                    target_object = 'Opportunity'
                elif id_prefix == '00Q':  # Lead
                    target_object = 'Lead'
                
                # Replace with appropriate default record if available
                if target_object and target_object in default_record_ids:
                    modified_df.loc[idx, field_name] = default_record_ids[target_object]
                    replacement_count += 1
                elif target_object:
                    # Clear the field if we know the object type but don't have a default record
                    modified_df.loc[idx, field_name] = None
                    print(f"    Cleared {field_name} value (no default {target_object} record available)")
            
            if replacement_count > 0:
                print(f"  Replaced {replacement_count} {field_name} values with appropriate default record IDs based on ID prefixes")
            continue
        
        # Default behavior for all other objects and fields
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

def filter_out_default_records(df, obj_name, default_records):
    """Remove default records from DataFrame if they exist in CSV data."""
    if obj_name not in default_records:
        return df
    
    default_record_data = default_records[obj_name]
    original_count = len(df)
    
    # Create a mask to identify default records
    is_default_mask = pd.Series([True] * len(df))
    
    # Check each field in the default record definition
    for field_name, expected_value in default_record_data.items():
        if field_name in df.columns:
            # Records must match ALL fields to be considered default records
            field_mask = df[field_name] == expected_value
            is_default_mask = is_default_mask & field_mask
    
    # Filter out default records
    filtered_df = df[~is_default_mask].copy()
    removed_count = original_count - len(filtered_df)
    
    if removed_count > 0:
        print(f"  Removed {removed_count} default {obj_name} record(s) from CSV data (already created in Apex)")
    
    return filtered_df

def save_id_mapping(obj_name, original_ids, new_ids):
    """Save the ID mapping to a CSV file for reference and future use."""
    if not new_ids or len(original_ids) != len(new_ids):
        print(f"  Warning: Cannot save ID mapping for {obj_name} - mismatched ID arrays")
        return
    
    # Create mapping DataFrame
    mapping_df = pd.DataFrame({
        'Id': original_ids,
        'NewId': new_ids
    })
    
    # Remove rows where NewId is None (failed inserts)
    mapping_df = mapping_df.dropna(subset=['NewId'])
    
    # Ensure mapping_data directory exists
    os.makedirs('mapping_data', exist_ok=True)
    
    # Save to CSV in mapping_data folder
    mapping_file = os.path.join('mapping_data', f"id_mapping_{obj_name}.csv")
    mapping_df.to_csv(mapping_file, index=False)
    print(f"  Saved {len(mapping_df)} ID mappings to {mapping_file}")
    
    return mapping_df

def load_all_id_mappings():
    """Load all existing ID mappings from CSV files."""
    mappings = {}
    import glob
    
    # Find all ID mapping files in mapping_data folder
    mapping_files = glob.glob("mapping_data/id_mapping_*.csv")
    
    for file in mapping_files:
        # Extract object name from filename
        filename = os.path.basename(file)
        obj_name = filename.replace("id_mapping_", "").replace(".csv", "")
        
        try:
            df = pd.read_csv(file)
            if 'Id' in df.columns and 'NewId' in df.columns:
                # Create a dictionary mapping old ID to new ID
                mappings[obj_name] = dict(zip(df['Id'], df['NewId']))
                print(f"  Loaded {len(mappings[obj_name])} ID mappings for {obj_name}")
            else:
                print(f"  Warning: Invalid mapping file format for {file}")
        except Exception as e:
            print(f"  Warning: Could not load ID mappings from {file}: {e}")
    
    return mappings

def update_all_lookup_fields(sf, lookup_mappings, all_id_mappings, import_order):
    """Update all lookup fields with correct new IDs after all imports are complete."""
    print("--- Updating All Lookup Fields with New IDs ---")
    
    data_dir = "exported_data"
    
    for obj_name in import_order:
        if obj_name not in lookup_mappings or obj_name not in all_id_mappings:
            continue
            
        print(f"Updating lookup fields for {obj_name}...")
        
        # Load the original CSV data to get the original relationships
        csv_path = os.path.join(data_dir, f"{obj_name}.csv")
        if not os.path.exists(csv_path):
            print(f"  CSV file not found for {obj_name}, skipping.")
            continue
            
        try:
            original_df = pd.read_csv(csv_path)
            if 'Id' not in original_df.columns:
                print(f"  'Id' column not found in {obj_name} CSV, skipping.")
                continue
        except Exception as e:
            print(f"  Error reading {obj_name} CSV: {e}")
            continue
        
        # Get ID mappings for this object
        object_id_mapping = all_id_mappings[obj_name]
        
        if not object_id_mapping:
            print(f"  No ID mappings found for {obj_name}, skipping.")
            continue
        
        # Get lookup fields for this object
        object_lookup_fields = lookup_mappings[obj_name]
        
        # Process each lookup field
        for field_name, field_info in object_lookup_fields.items():
            # Skip non-updateable fields
            if not field_info.get('updateable', False):
                continue
                
            # Skip if field is not in the original CSV
            if field_name not in original_df.columns:
                continue
                
            referenced_objects = field_info.get('referenceTo', [])
            
            # Special handling for Task object lookup fields using ID prefixes
            if obj_name == 'Task' and field_name in ['WhatId', 'WhoId']:
                print(f"  Processing {field_name} with ID prefix detection...")
                
                # Group records by the object type they reference (based on ID prefix)
                records_by_object_type = {}
                
                for _, row in original_df.iterrows():
                    original_record_id = row['Id']
                    original_lookup_value = row.get(field_name)
                    
                    # Skip if no lookup value or lookup value is empty
                    if pd.isna(original_lookup_value) or original_lookup_value == '' or original_lookup_value == ' ':
                        continue
                    
                    # Get the new ID for this record
                    if original_record_id not in object_id_mapping:
                        continue
                    new_record_id = object_id_mapping[original_record_id]
                    
                    # Determine object type from ID prefix
                    if not isinstance(original_lookup_value, str) or len(original_lookup_value) < 3:
                        continue
                        
                    id_prefix = original_lookup_value[:3]
                    target_object = None
                    
                    if id_prefix == '001':  # Account
                        target_object = 'Account'
                    elif id_prefix == '006':  # Opportunity
                        target_object = 'Opportunity'
                    elif id_prefix == '00Q':  # Lead
                        target_object = 'Lead'
                    
                    # Only proceed if we have ID mappings for this object type
                    if target_object and target_object in all_id_mappings:
                        ref_id_mapping = all_id_mappings[target_object]
                        
                        # Get the new ID for the referenced record
                        if original_lookup_value in ref_id_mapping:
                            new_lookup_value = ref_id_mapping[original_lookup_value]
                            
                            # Group by target object type
                            if target_object not in records_by_object_type:
                                records_by_object_type[target_object] = []
                            
                            records_by_object_type[target_object].append({
                                'Id': new_record_id,
                                field_name: new_lookup_value
                            })
                        else:
                            print(f"    Warning: Referenced {target_object} ID {original_lookup_value} not found in mappings")
                
                # Update records grouped by object type
                for target_object, records_to_update in records_by_object_type.items():
                    if records_to_update:
                        print(f"    Updating {len(records_to_update)} {field_name} references to {target_object}...")
                        
                        # Update in batches
                        batch_size = 200
                        for i in range(0, len(records_to_update), batch_size):
                            batch = records_to_update[i:i + batch_size]
                            try:
                                update_results = sf.bulk.__getattr__(obj_name).update(batch)
                                successful_updates = sum(1 for result in update_results if result.get('success'))
                                failed_updates = len(batch) - successful_updates
                                print(f"      Batch {i//batch_size + 1}: {successful_updates}/{len(batch)} records updated successfully")
                                
                                if failed_updates > 0:
                                    print(f"        {failed_updates} updates failed")
                                    # Show detailed error information for failed updates
                                    for j, result in enumerate(update_results):
                                        if not result.get('success'):
                                            record_data = batch[j] if j < len(batch) else {}
                                            print(f"          Failed update #{j+1}:")
                                            print(f"            Record ID: {record_data.get('Id', 'Unknown')}")
                                            print(f"            Field: {field_name} = {record_data.get(field_name, 'Unknown')}")
                                            
                                            # Extract detailed error information
                                            if 'error' in result:
                                                print(f"            Error: {result['error']}")
                                            
                                            if 'errors' in result:
                                                if isinstance(result['errors'], list):
                                                    for error in result['errors']:
                                                        if isinstance(error, dict):
                                                            error_msg = error.get('message', str(error))
                                                            error_code = error.get('statusCode', '')
                                                            error_fields = error.get('fields', [])
                                                            print(f"            Error Code: {error_code}")
                                                            print(f"            Error Message: {error_msg}")
                                                            if error_fields:
                                                                print(f"            Error Fields: {', '.join(error_fields)}")
                                                        else:
                                                            print(f"            Error: {error}")
                                                else:
                                                    print(f"            Errors: {result['errors']}")
                                            
                                            # If no specific errors found, show the full result
                                            if 'error' not in result and 'errors' not in result:
                                                print(f"            Full result: {result}")
                                            
                                            print()  # Empty line for readability
                                            
                                            # Limit to first 3 failures to avoid spam
                                            if j >= 2:
                                                remaining_failures = failed_updates - 3
                                                if remaining_failures > 0:
                                                    print(f"          ... and {remaining_failures} more failed updates")
                                                break
                                                
                            except Exception as e:
                                print(f"      Batch {i//batch_size + 1} failed with exception: {e}")
                                print(f"        Exception type: {type(e).__name__}")
                                if hasattr(e, 'content'):
                                    print(f"        Exception content: {e.content}")
                                if hasattr(e, 'url'):
                                    print(f"        Request URL: {e.url}")
                    else:
                        print(f"    No {field_name} fields need updating for {target_object}")
                
                continue  # Skip the default logic for Task WhatId/WhoId fields
            
            # Default behavior for all other objects and fields
            # Check if we have ID mappings for the referenced objects
            for ref_object in referenced_objects:
                if ref_object in all_id_mappings:
                    ref_id_mapping = all_id_mappings[ref_object]
                    
                    print(f"  Processing {field_name} references to {ref_object}...")
                    
                    # Build the updates based on original CSV relationships
                    records_to_update = []
                    
                    for _, row in original_df.iterrows():
                        original_record_id = row['Id']
                        original_lookup_value = row.get(field_name)
                        
                        # Skip if no lookup value or lookup value is empty
                        if pd.isna(original_lookup_value) or original_lookup_value == '' or original_lookup_value == ' ':
                            continue
                            
                        # Get the new ID for this record
                        if original_record_id not in object_id_mapping:
                            continue
                        new_record_id = object_id_mapping[original_record_id]
                        
                        # Get the new ID for the referenced record
                        if original_lookup_value not in ref_id_mapping:
                            print(f"    Warning: Referenced {ref_object} ID {original_lookup_value} not found in mappings")
                            continue
                        new_lookup_value = ref_id_mapping[original_lookup_value]
                        
                        # Add to update list
                        records_to_update.append({
                            'Id': new_record_id,
                            field_name: new_lookup_value
                        })
                    
                    if records_to_update:
                        print(f"    Updating {len(records_to_update)} records with new {ref_object} IDs...")
                        
                        # Update in batches
                        batch_size = 200
                        for i in range(0, len(records_to_update), batch_size):
                            batch = records_to_update[i:i + batch_size]
                            try:
                                update_results = sf.bulk.__getattr__(obj_name).update(batch)
                                successful_updates = sum(1 for result in update_results if result.get('success'))
                                failed_updates = len(batch) - successful_updates
                                print(f"      Batch {i//batch_size + 1}: {successful_updates}/{len(batch)} records updated successfully")
                                
                                if failed_updates > 0:
                                    print(f"        {failed_updates} updates failed")
                                    # Show detailed error information for failed updates
                                    for j, result in enumerate(update_results):
                                        if not result.get('success'):
                                            record_data = batch[j] if j < len(batch) else {}
                                            print(f"          Failed update #{j+1}:")
                                            print(f"            Record ID: {record_data.get('Id', 'Unknown')}")
                                            print(f"            Field: {field_name} = {record_data.get(field_name, 'Unknown')}")
                                            
                                            # Extract detailed error information
                                            if 'error' in result:
                                                print(f"            Error: {result['error']}")
                                            
                                            if 'errors' in result:
                                                if isinstance(result['errors'], list):
                                                    for error in result['errors']:
                                                        if isinstance(error, dict):
                                                            error_msg = error.get('message', str(error))
                                                            error_code = error.get('statusCode', '')
                                                            error_fields = error.get('fields', [])
                                                            print(f"            Error Code: {error_code}")
                                                            print(f"            Error Message: {error_msg}")
                                                            if error_fields:
                                                                print(f"            Error Fields: {', '.join(error_fields)}")
                                                        else:
                                                            print(f"            Error: {error}")
                                                else:
                                                    print(f"            Errors: {result['errors']}")
                                            
                                            # If no specific errors found, show the full result
                                            if 'error' not in result and 'errors' not in result:
                                                print(f"            Full result: {result}")
                                            
                                            print()  # Empty line for readability
                                            
                                            # Limit to first 3 failures to avoid spam
                                            if j >= 2:
                                                remaining_failures = failed_updates - 3
                                                if remaining_failures > 0:
                                                    print(f"          ... and {remaining_failures} more failed updates")
                                                break
                                            
                            except Exception as e:
                                print(f"      Batch {i//batch_size + 1} failed with exception: {e}")
                                print(f"        Exception type: {type(e).__name__}")
                                if hasattr(e, 'content'):
                                    print(f"        Exception content: {e.content}")
                                if hasattr(e, 'url'):
                                    print(f"        Request URL: {e.url}")
                    else:
                        print(f"    No {field_name} fields need updating for {ref_object}")
                    
                    break  # Only process the first matching reference type

def main():
    """Main function to handle the data import process."""
    # Set up logging first
    logger = setup_logging()
    
    # Clear any existing describe cache to ensure fresh data
    clear_describe_cache()
    
    sf = get_salesforce_connection()
    if not sf:
        logger.error("Failed to connect to Salesforce. Exiting.")
        print("Failed to connect to Salesforce. Exiting.")
        return

    logger.info("Successfully connected to Salesforce for import.")
    print("Successfully connected to Salesforce for import.")

    # --- Define Import Order (from configuration) ---
    import_order = OBJECTS_LIST

    parser = argparse.ArgumentParser(description='Import Salesforce data from CSV files.')
    parser.add_argument('--object', type=str, help='The specific Salesforce object to import (e.g., Account). If not provided, all objects will be imported.')
    parser.add_argument('--update-lookups', action='store_true', help='Update lookup fields with new IDs after import (run this after all imports are complete).')
    args = parser.parse_args()

    # If --update-lookups flag is provided, only run the lookup update process
    if args.update_lookups:
        logger.info("Starting lookup field update process")
        # Generate lookup field mappings for current org (always get fresh data)
        logger.info("Generating lookup field mappings from current org")
        lookup_mappings = generate_lookup_field_mappings(sf, import_order)
        
        # Load existing ID mappings from previous imports
        logger.info("Loading existing ID mappings")
        print("--- Loading Existing ID Mappings ---")
        all_id_mappings = load_all_id_mappings()
        
        if all_id_mappings and lookup_mappings:
            update_all_lookup_fields(sf, lookup_mappings, all_id_mappings, import_order)
            logger.info("Lookup field update process completed")
        else:
            logger.warning("No ID mappings or lookup field mappings found. Import data first.")
            print("No ID mappings or lookup field mappings found. Import data first.")
        return

    # Load and create default records (only when importing, not when updating lookups)
    logger.info("Loading default records configuration")
    default_records = load_default_records()
    default_record_ids = {}
    if default_records:
        logger.info("Creating default records in Salesforce")
        default_record_ids = create_default_records(sf, default_records)

    # Generate lookup field mappings for current org (always get fresh data)
    logger.info("Generating lookup field mappings from current org")
    lookup_mappings = generate_lookup_field_mappings(sf, import_order)
    
    # Load existing ID mappings from previous imports
    logger.info("Loading existing ID mappings")
    print("--- Loading Existing ID Mappings ---")
    all_id_mappings = load_all_id_mappings()

    data_dir = "exported_data"
    objects_to_process = []

    if args.object:
        # If a specific object is provided, only process that one.
        if args.object in import_order:
            objects_to_process = [args.object]
            logger.info(f"Processing single object: {args.object}")
        else:
            logger.error(f"Object '{args.object}' is not in the defined import_order list.")
            print(f"Error: Object '{args.object}' is not in the defined import_order list.")
            return
    else:
        # Otherwise, process all objects defined in the import order.
        objects_to_process = import_order
        logger.info(f"Processing all objects: {', '.join(import_order)}")

    id_map = {}
    total_objects = len(objects_to_process)
    processed_objects = 0

    for obj_name in objects_to_process:
        processed_objects += 1
        logger.info(f"Processing object {processed_objects}/{total_objects}: {obj_name}")
        
        csv_path = os.path.join(data_dir, f"{obj_name}.csv")
        if not os.path.exists(csv_path):
            logger.warning(f"CSV file not found for {obj_name}, skipping.")
            print(f"CSV file not found for {obj_name}, skipping.")
            continue
        print(f"--- Processing {obj_name} --- ")
        df = read_csv_with_string_fields_preserved(sf, obj_name, csv_path)

        if 'Id' not in df.columns:
            logger.error(f"'Id' column not found in {csv_path}, skipping.")
            print(f"'Id' column not found in {csv_path}, skipping.")
            continue

        # Store original IDs
        original_ids = df['Id'].tolist()

        # Filter out default records if they exist in CSV (they're created in Apex)
        default_records = load_default_records()
        if default_records:
            logger.info(f"Filtering out default records for {obj_name}")
            print(f"  Filtering out default records...")
            df = filter_out_default_records(df, obj_name, default_records)
            
            # If all records were filtered out, skip this object
            if len(df) == 0:
                logger.info(f"No records remaining after filtering default records for {obj_name}, skipping.")
                print(f"  No records remaining after filtering default records for {obj_name}, skipping.")
                continue
            
            # Update original_ids list after filtering
            original_ids = df['Id'].tolist()

        logger.info(f"Starting data processing for {obj_name} with {len(df)} records")

        # Clean data for insertion
        readonly_fields = get_readonly_fields(sf, obj_name)
        available_fields = get_available_fields(sf, obj_name)
        
        # Find fields in CSV that don't exist in current org
        csv_fields = set(df.columns)
        missing_fields = csv_fields - available_fields
        
        if missing_fields:
            print(f"  Warning: {len(missing_fields)} fields in CSV not found in current {obj_name} object:")
            for field in sorted(list(missing_fields)[:10]):  # Show first 10 to avoid spam
                print(f"    {field}")
            if len(missing_fields) > 10:
                print(f"    ... and {len(missing_fields) - 10} more fields")
        
        # Also remove the original Id field itself and any missing fields
        fields_to_drop = list(readonly_fields.intersection(df.columns)) + ['Id'] + list(missing_fields)
        insert_df = df.drop(columns=fields_to_drop, errors='ignore')

        # Replace lookup fields with default record IDs (no ID mapping yet)
        if default_record_ids and lookup_mappings:
            print(f"  Replacing lookup fields with default record IDs...")
            insert_df = replace_lookup_fields_with_defaults(sf, obj_name, insert_df, default_record_ids, lookup_mappings)

        # Validate and replace non-existent User IDs
        print(f"  Validating User IDs...")
        insert_df = validate_and_replace_user_ids(sf, obj_name, insert_df)

        # Fix text field formatting to prevent unwanted float conversion
        print(f"  Fixing text field formatting...")
        insert_df = fix_text_field_formatting(sf, obj_name, insert_df)

        # Clean lookup field references that point to non-existent records
        if lookup_mappings:
            print(f"  Validating lookup field references...")
            insert_df = clean_lookup_references(sf, obj_name, insert_df, lookup_mappings)

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
                logger.info(f"Bulk operation for {obj_name}: {successful_inserts} successful, {len(bulk_results) - successful_inserts} failed")
                
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
                # Save ID mapping to CSV file
                save_id_mapping(obj_name, valid_original_ids, valid_new_ids)
                
                # Update the all_id_mappings for use in subsequent objects
                all_id_mappings[obj_name] = dict(zip(valid_original_ids, valid_new_ids))
                
                # Keep legacy id_map for compatibility (if needed)
                id_map[f"{obj_name}Id"] = dict(zip(valid_original_ids, valid_new_ids))
                
                logger.info(f"Successfully inserted {successful_inserts} of {len(records_to_insert)} records for {obj_name}")
                print(f"  Successfully inserted {successful_inserts} of {len(records_to_insert)} records for {obj_name}.")
            else:
                logger.warning(f"No records were successfully inserted for {obj_name}")
                print(f"  No records were successfully inserted for {obj_name}.")

        except Exception as e:
            logger.error(f"Error during bulk insert for {obj_name}: {e}")
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
                # Save ID mapping to CSV file
                save_id_mapping(obj_name, valid_original_ids, valid_new_ids)
                
                # Update the all_id_mappings for use in subsequent objects
                all_id_mappings[obj_name] = dict(zip(valid_original_ids, valid_new_ids))
                
                # Keep legacy id_map for compatibility (if needed)
                id_map[f"{obj_name}Id"] = dict(zip(valid_original_ids, valid_new_ids))
                
                logger.info(f"Successfully inserted {successful_inserts} of {len(records_to_insert)} records for {obj_name} (fallback method)")
                print(f"  Successfully inserted {successful_inserts} of {len(records_to_insert)} records for {obj_name} (fallback method).")
            else:
                logger.warning(f"No records were successfully inserted for {obj_name} (fallback method)")
                print(f"  No records were successfully inserted for {obj_name}.")

    # After all imports are complete, remind user to update lookup fields
    if not args.object:  # Only show this message when importing all objects
        logger.info("Import process completed successfully")
        print("\n" + "="*60)
        print("IMPORT COMPLETE!")
        print("="*60)
        print("To update lookup fields with correct relationships, run:")
        print("python3 salesforce_importer.py --update-lookups")
        print("="*60)
    else:
        logger.info(f"Single object import completed for {args.object}")

if __name__ == "__main__":
    main()
