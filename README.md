# Salesforce Sandbox Data Restoration Tool

A comprehensive Python tool for backing up and restoring Salesforce data after sandbox refreshes. This tool handles the complete workflow from data export to import with advanced relationship handling and ID mapping capab| `salesforce_exporter.py` | Exports data from source org before sandbox refresh |
| `salesforce_importer.py` | Main import script with automatic lookup field analysis |
| `objects_config.py` | Shared configuration defining objects to process |
| `default_records.json` | Defines default records for lookup field replacement |ties.

## Overview

This tool provides a complete sandbox data restoration workflow:

1. **Export Phase**: Extract data from production/source org before sandbox refresh
2. **Import Phase 1**: Import all records with default lookup values (eliminates dependency order issues)
3. **Import Phase 2**: Update all lookup fields with correct relationships using ID mappings

**Primary Use Case**: Restore your sandbox data after a production refresh, maintaining all relationships and references.

## Prerequisites

### Required Software

- Python 3.7+
- pip (Python package manager)

### Required Python Packages

All dependencies are managed through `requirements.txt`. See setup instructions below for installation.

### Salesforce Requirements

- Salesforce org with API access
- Connected App with OAuth credentials
- Appropriate user permissions for data import/export

## Setup

### 1. Clone/Download Project

```bash
# Download or clone the project to your local machine
cd /path/to/your/workspace
# Ensure you have all project files including requirements.txt
```

### 2. Python Virtual Environment Setup

Create and activate a virtual environment to isolate dependencies:

**On macOS/Linux:**

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip3 install -r requirements.txt
```

**On Windows:**

```cmd
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
venv\Scripts\activate

# Install dependencies
pip3 install -r requirements.txt
```

### 3. Environment Configuration

Base on `.env.sample`, create a `.env` file in the project root:

```bash
SALESFORCE_USERNAME=your_username@domain.com
SALESFORCE_PASSWORD=your_password_with_security_token
SALESFORCE_CONSUMER_KEY=your_connected_app_consumer_key
SALESFORCE_CONSUMER_SECRET=your_connected_app_consumer_secret
SALESFORCE_DOMAIN=login
```

### 4. Project Structure

Ensure your project follows this structure:

```
BackupSandbox/
├── .env                           # Salesforce credentials
├── venv/                          # Python virtual environment (created by you)
├── requirements.txt               # Python dependencies
├── salesforce_exporter.py         # Export script for data backup
├── salesforce_importer.py         # Main import script with lookup field analysis
├── objects_config.py              # Shared object configuration
├── default_records.json          # Default record definitions
├── lookup_field_mappings.json    # Lookup field mappings
├── exported_data/                # CSV files to import
│   ├── Account.csv
│   ├── Lead.csv
│   ├── Task.csv
│   ├── Opportunity.csv
│   └── ...
└── mapping_data/                  # ID mapping files (auto-generated)
    ├── id_mapping_Account.csv
    ├── id_mapping_Lead.csv
    ├── id_mapping_Task.csv
    └── ...
```

### 5. Configuration Files

#### objects_config.py

Defines the objects to be exported and imported:

```python
OBJECTS_LIST = [
    'Account',      # Parent object - must come first
    'Lead',         # Independent object
    'Task',         # Can reference Account, Lead, Opportunity
    'Opportunity',  # References Account
    'Apart__c',     # Custom object
    'Room__c',      # References Apart__c
    'Buyer__c',     # Custom object
    'Transcript__c', # Custom object
    'MP_Action__c', # Custom object - references Lead, Opportunity
    'OpportunityLog__c', # Custom object - references Opportunity
    'ValuationLog__c'    # Custom object - references Opportunity
]
```

To add or remove objects, simply modify this list. Order matters for import due to lookup relationships.

#### default_records.json

Define default records for lookup fields:

```json
{
  "Account": {
    "Name": "Default Account",
    "Type": "Other"
  },
  "Apart__c": {
    "Name": "Default Apartment"
  },
  "Room__c": {
    "Name": "Default Room"
  }
}
```

## Usage

### Complete Sandbox Restoration Workflow

#### Before Sandbox Refresh: Export Data

```bash
# Activate virtual environment
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate     # On Windows

# Export data from your source org (before refresh)
python3 salesforce_exporter.py
```

This exports all your data to CSV files in `exported_data/` folder.

#### After Sandbox Refresh: Restore Data

### Step 1: Prepare Salesforce Org

Before importing data, ensure your refreshed sandbox is properly configured:

#### 1.1 Deactivate Flows

Deactivate these flows to prevent interference during data import:

- **MP_001_Send Action Notification Slack**
- **MP_009_KNB の MP アクション作成時、活動登録をする**

**How to deactivate:**

1. Go to Setup → Flow → Flow Builder
2. Search for each flow name
3. Click on the flow and select "Deactivate"
4. Confirm deactivation

#### 1.2 Grant Lead Permissions

Grant the following permission to the user profile used for data import:

- **View and Edit Converted Leads**

**How to grant permission:**

1. Go to Setup → Users → Profiles
2. Find and edit the profile of your import user
3. Go to Object Settings → Leads
4. Enable "View and Edit Converted Leads" permission
5. Save the profile changes

### Step 2: Import Data (Phase 1)

Import all records with default lookup values:

```bash
# Import all objects (automatically analyzes lookup fields)
python3 salesforce_importer.py

# Import specific object
python3 salesforce_importer.py --object Account
```

**What happens:**

- Automatically analyzes current org to generate lookup field mappings
- Creates default records in Salesforce
- Imports all CSV data with lookup fields pointing to defaults
- Generates ID mapping files in `mapping_data/` folder
- Shows detailed progress and error information

### Step 3: Update Relationships (Phase 2)

Restore original relationships using ID mappings:

```bash
python3 salesforce_importer.py --update-lookups
```

**What happens:**

- Loads original CSV data to identify relationships
- Maps old IDs to new IDs using mapping files
- Updates all lookup fields with correct relationships
- Provides detailed error reporting for failed updates

## Troubleshooting

### Common Issues

#### 1. Authentication Errors

```
Error: INVALID_LOGIN
```

**Solution**: Check your `.env` file credentials and security token.

#### 2. Field Not Found Errors

```
Warning: X fields in CSV not found in current ObjectName object
```

**Solution**: This is normal - the tool automatically handles missing fields.

#### 3. Lookup Field Update Failures

```
Error Code: INVALID_FIELD_FOR_INSERT_UPDATE
```

**Solution**: Check that referenced records exist and field is updateable.

#### 4. Bulk API Limits

```
Bulk API error: REQUEST_LIMIT_EXCEEDED
```

**Solution**: The tool automatically falls back to single-record insert.

### Performance Tips

- **Use Bulk API**: Enabled by default for better performance
- **Batch Processing**: Records processed in batches of 200
- **Parallel Safe**: Can run multiple object imports simultaneously
- **Resume Capability**: Can restart failed imports using existing mappings

## File Descriptions

| File                         | Purpose                                                 |
| ---------------------------- | ------------------------------------------------------- |
| `salesforce_exporter.py`     | Exports data from source org before sandbox refresh     |
| `salesforce_importer.py`     | Main import script with automatic lookup field analysis |
| `default_records.json`       | Defines default records for lookup field replacement    |
| `lookup_field_mappings.json` | Contains field metadata and relationship information    |
| `requirements.txt`           | Python package dependencies                             |
| `exported_data/*.csv`        | Source data files to import                             |
| `mapping_data/*.csv`         | Generated ID mappings (old→new)                         |
