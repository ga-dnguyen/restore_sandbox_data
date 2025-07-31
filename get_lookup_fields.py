import os
import json
from dotenv import load_dotenv
from simple_salesforce import Salesforce

def get_salesforce_connection():
    """Establishes and returns a Salesforce connection."""
    load_dotenv(override=True)
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

def get_lookup_relationships(sf, object_name):
    """Get all lookup relationships for an object"""
    try:
        sobject_desc = getattr(sf, object_name).describe()
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
    except Exception as e:
        print(f"Error describing {object_name}: {e}")
        return {}

def main():
    sf = get_salesforce_connection()
    
    # Objects to analyze
    objects_to_analyze = [
        'Account', 'Lead', 'Task', 'Opportunity', 'Apart__c', 
        'Room__c', 'Buyer__c', 'Transcript__c', 'MP_Action__c', 
        'OpportunityLog__c', 'ValuationLog__c'
    ]
    
    all_lookup_mappings = {}
    
    for obj_name in objects_to_analyze:
        print(f"\n--- Analyzing {obj_name} ---")
        lookup_fields = get_lookup_relationships(sf, obj_name)
        
        if lookup_fields:
            all_lookup_mappings[obj_name] = lookup_fields
            print(f"Found {len(lookup_fields)} lookup fields:")
            for field_name, details in lookup_fields.items():
                referenced_objects = ', '.join(details['referenceTo'])
                print(f"  {field_name} → {referenced_objects}")
        else:
            print(f"No lookup fields found for {obj_name}")
    
    # Save to JSON file for reference
    with open('lookup_field_mappings.json', 'w') as f:
        json.dump(all_lookup_mappings, f, indent=2)
    
    print(f"\nLookup field mappings saved to lookup_field_mappings.json")

if __name__ == "__main__":
    main()
