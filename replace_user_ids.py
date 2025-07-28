import os
import pandas as pd
from simple_salesforce import Salesforce
from dotenv import load_dotenv

def get_salesforce_connection():
    load_dotenv(override=True)
    sf_username = os.getenv('SALESFORCE_USERNAME')
    sf_password = os.getenv('SALESFORCE_PASSWORD')
    sf_consumer_key = os.getenv('SALESFORCE_CONSUMER_KEY')
    sf_consumer_secret = os.getenv('SALESFORCE_CONSUMER_SECRET')
    sf_domain = os.getenv('SALESFORCE_DOMAIN', 'login')
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

def get_user_ids_by_emails(sf, emails):
    email_str = ",".join([f"'{email}'" for email in emails])
    query = f"SELECT Id, Email FROM User WHERE Email IN ({email_str})"
    users = sf.query_all(query)['records']
    return {user['Email']: user['Id'] for user in users}

def replace_user_ids_in_csvs(user_ids, new_id, data_dir):
    for fname in os.listdir(data_dir):
        if fname.endswith('.csv'):
            fpath = os.path.join(data_dir, fname)
            df = pd.read_csv(fpath, dtype=str)
            changed = False
            for col in df.columns:
                if df[col].isin(user_ids.values()).any():
                    df[col] = df[col].replace(user_ids.values(), new_id)
                    changed = True
            if changed:
                df.to_csv(fpath, index=False)
                print(f"Updated {fname}")

if __name__ == "__main__":
    # List of emails to replace
    emails = [
        'truong.phamvan@vti.com.vn',
        'hang.vuthi1@vti.com.vn',
        'huong.nguyenthi2@vti.com.vn',
        'tuan.phananh@vti.com.vn',
        'hien.nguyenthi1@vti.com.vn',
        'tuyen.laivan@vti.com.vn',
        'linh.nguyenmanh@vti.com.vn',
    ]
    new_id = '0052j000000kxjEAAQ'
    data_dir = 'exported_data'
    sf = get_salesforce_connection()
    user_ids = get_user_ids_by_emails(sf, emails)
    print(f"User IDs to replace: {user_ids}")
    replace_user_ids_in_csvs(user_ids, new_id, data_dir)
