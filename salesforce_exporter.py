import os
import pandas as pd
from dotenv import load_dotenv
from simple_salesforce import Salesforce

def main():
    """
    Connects to Salesforce, queries specified objects, and saves the data to CSV files.
    """
    # Load environment variables from .env file
    load_dotenv(override=True)

    # Salesforce OAuth credentials
    sf_username = os.getenv("SALESFORCE_USERNAME")
    sf_password = os.getenv("SALESFORCE_PASSWORD")
    sf_consumer_key = os.getenv("SALESFORCE_CONSUMER_KEY")
    sf_consumer_secret = os.getenv("SALESFORCE_CONSUMER_SECRET")
    sf_domain = os.getenv("SALESFORCE_DOMAIN", "login")

    # List of objects to query
    objects_to_query = [
        'Lead',
        'Task',
        'Opportunity',
        'Account',
        'MP_Action__c',
        'OpportunityLog__c',
        'ValuationLog__c',
        'Apart__c',
        'Room__c',
        'Buyer__c',
        'Transcript__c'
    ]

    # Ensure the exported_data and exported_metadata directories exist
    data_dir = "exported_data"
    metadata_dir = "exported_metadata"
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(metadata_dir, exist_ok=True)

    try:
        # Connect to Salesforce using OAuth2
        sf = Salesforce(
            username=sf_username,
            password=sf_password,
            consumer_key=sf_consumer_key,
            consumer_secret=sf_consumer_secret,
            domain=sf_domain
        )
        print("Successfully connected to Salesforce via OAuth.")

        for obj_name in objects_to_query:
            print(f"Querying object: {obj_name}...")
            try:
                # Get all field names for the object
                sobject = getattr(sf, obj_name)
                sobject_desc = sobject.describe()
                field_names = [field['name'] for field in sobject_desc['fields']]

                # Export metadata: field API name and type
                metadata_rows = [
                    {"api_name": field['name'], "type": field['type']} for field in sobject_desc['fields']
                ]
                metadata_df = pd.DataFrame(metadata_rows)
                metadata_file_name = os.path.join(metadata_dir, f"{obj_name}.csv")
                metadata_df.to_csv(metadata_file_name, index=False)
                print(f"Successfully saved metadata for {obj_name} to {metadata_file_name}")

                # Construct the SOQL query
                soql_query = f"SELECT {', '.join(field_names)} FROM {obj_name}"
                
                # Execute the query
                query_result = sf.query_all(soql_query)
                
                # Check if there are any records
                if query_result['records']:
                    # Convert the result to a pandas DataFrame
                    df = pd.DataFrame(query_result['records'])
                    
                    # Remove 'attributes' column if it exists
                    if 'attributes' in df.columns:
                        df = df.drop(columns='attributes')
                    
                    # Save the DataFrame to a CSV file in the exported_data directory
                    csv_file_name = os.path.join(data_dir, f"{obj_name}.csv")
                    df.to_csv(csv_file_name, index=False)
                    print(f"Successfully saved data for {obj_name} to {csv_file_name}")
                else:
                    # No records found, create an empty CSV with just headers
                    empty_df = pd.DataFrame(columns=field_names)
                    csv_file_name = os.path.join(data_dir, f"{obj_name}.csv")
                    empty_df.to_csv(csv_file_name, index=False)
                    print(f"No records found for {obj_name}. Created empty CSV file: {csv_file_name}")

            except Exception as e:
                print(f"An error occurred while processing {obj_name}: {e}")

    except Exception as e:
        print(f"Failed to connect to Salesforce: {e}")

if __name__ == "__main__":
    main()
