# Salesforce Sandbox Data Restoration Tool

A comprehensive Python tool for backing up and restoring Salesforce data after sandbox refreshes. This tool handles the complete workflow from data export to import with advanced relationship handling and ID mapping capabilities.

## Overview

This tool provides a complete sandbox data restoration workflow:

1. **Export Phase**: Extract data from production/source org before sandbox refresh
2. **Import Phase 1**: Import all records with default lookup values (eliminates dependency order issues)
3. **Import Phase 2**: Update all lookup fields with correct relationships using ID mappings

**Primary Use Case**: Restore your sandbox data after a production refresh, maintaining all relationships and references.

## Features

- ✅ **Complete Data Restoration**: Export from source org and import to refreshed sandbox
- ✅ **Two-Phase Import**: Eliminates complex dependency ordering
- ✅ **ID Mapping System**: Tracks old→new ID relationships in CSV files
- ✅ **Default Record Management**: Creates and manages default records for lookup fields
- ✅ **Bulk API Integration**: High-performance exports and imports using Salesforce Bulk API
- ✅ **Comprehensive Error Handling**: Detailed error reporting and fallback mechanisms
- ✅ **Field Validation**: Automatic detection and handling of missing/invalid fields
- ✅ **Relationship Restoration**: Accurate restoration of original data relationships
- ✅ **Virtual Environment Support**: Isolated Python environment for dependency management

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

Create a `.env` file in the project root:

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
├── salesforce_importer.py         # Main import script
├── get_lookup_fields.py           # Lookup field analyzer
├── replace_user_ids.py            # User ID replacement utility
├── default_records.json          # Default record definitions
├── lookup_field_mappings.json    # Lookup field mappings
├── exported_data/                # CSV files to import
│   ├── Account.csv
│   ├── Lead.csv
│   ├── Task.csv
│   ├── Opportunity.csv
│   └── ...
├── exported_metadata/             # Metadata files (auto-generated)
└── mapping_data/                  # ID mapping files (auto-generated)
    ├── id_mapping_Account.csv
    ├── id_mapping_Lead.csv
    ├── id_mapping_Task.csv
    └── ...
```

### 5. Configuration Files

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

#### CSV Data Requirements

- All CSV files must have an `Id` column with original Salesforce IDs
- Place CSV files in the `exported_data/` folder
- Supported objects: Account, Lead, Task, Opportunity, Apart**c, Room**c, Buyer**c, Transcript**c, MP_Action**c, OpportunityLog**c, ValuationLog\_\_c

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

### Step 2: Analyze Lookup Fields (Optional)

Generate lookup field mappings for your refreshed org:

```bash
python3 get_lookup_fields.py
```

This creates `lookup_field_mappings.json` with field metadata.

### Step 3: Import Data (Phase 1)

Import all records with default lookup values:

```bash
# Import all objects
python3 salesforce_importer.py

# Import specific object
python3 salesforce_importer.py --object Account
```

**What happens:**

- Creates default records in Salesforce
- Imports all CSV data with lookup fields pointing to defaults
- Generates ID mapping files in `mapping_data/` folder
- Shows detailed progress and error information

### Step 4: Update Relationships (Phase 2)

Restore original relationships using ID mappings:

```bash
python3 salesforce_importer.py --update-lookups
```

**What happens:**

- Loads original CSV data to identify relationships
- Maps old IDs to new IDs using mapping files
- Updates all lookup fields with correct relationships
- Provides detailed error reporting for failed updates

## Advanced Usage

### Import Order

Objects are imported in dependency order:

1. Account
2. Lead
3. Task
4. Opportunity
5. Apart\_\_c
6. Room\_\_c
7. Buyer\_\_c
8. Transcript\_\_c
9. MP_Action\_\_c
10. OpportunityLog\_\_c
11. ValuationLog\_\_c

### Error Handling

The tool provides comprehensive error handling:

- **Field Validation**: Automatically removes non-existent fields
- **Bulk API Fallback**: Falls back to single-record insert if bulk fails
- **Detailed Error Reports**: Shows specific field errors and record context
- **Partial Success**: Continues processing even if some records fail

### ID Mapping System

- **Automatic Generation**: Creates `id_mapping_ObjectName.csv` files
- **Format**: Two columns (`Id`, `NewId`) mapping old→new IDs
- **Persistent Storage**: Saved in `mapping_data/` folder
- **Reusable**: Can be used across multiple import sessions

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

### Debug Mode

For detailed debugging, check the console output which includes:

- Field validation results
- Bulk operation status
- Individual record error details
- ID mapping statistics

### Performance Tips

- **Use Bulk API**: Enabled by default for better performance
- **Batch Processing**: Records processed in batches of 200
- **Parallel Safe**: Can run multiple object imports simultaneously
- **Resume Capability**: Can restart failed imports using existing mappings

## File Descriptions

| File                         | Purpose                                              |
| ---------------------------- | ---------------------------------------------------- |
| `salesforce_exporter.py`     | Exports data from source org before sandbox refresh  |
| `salesforce_importer.py`     | Main import script with two-phase workflow           |
| `get_lookup_fields.py`       | Analyzes Salesforce org to generate lookup mappings  |
| `replace_user_ids.py`        | Utility to replace user IDs in exported data         |
| `default_records.json`       | Defines default records for lookup field replacement |
| `lookup_field_mappings.json` | Contains field metadata and relationship information |
| `requirements.txt`           | Python package dependencies                          |
| `exported_data/*.csv`        | Source data files to import                          |
| `exported_metadata/*.csv`    | Metadata files for field information                 |
| `mapping_data/*.csv`         | Generated ID mappings (old→new)                      |

## Example Workflow

### Complete Sandbox Restoration Process

```bash
# 1. Set up project
cd /path/to/BackupSandbox
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux
pip3 install -r requirements.txt

# 2. Set up environment
echo "SALESFORCE_USERNAME=user@company.com" > .env
echo "SALESFORCE_PASSWORD=password123token" >> .env
echo "SALESFORCE_CONSUMER_KEY=your_key" >> .env
echo "SALESFORCE_CONSUMER_SECRET=your_secret" >> .env

# 3. BEFORE sandbox refresh: Export your data
python3 salesforce_exporter.py

# 4. Refresh your sandbox (via Salesforce Setup)

# 5. AFTER refresh: Update .env with new sandbox credentials

# 6. Prepare Salesforce org (IMPORTANT!)
# - Deactivate flows: MP_001_Send Action Notification Slack and MP_009_KNBのMPアクション作成時、活動登録をする
# - Grant "View and Edit Converted Leads" permission to import user profile

# 7. Analyze your refreshed org (optional)
python3 get_lookup_fields.py

# 8. Restore all data
python3 salesforce_importer.py

# 9. Restore relationships
python3 salesforce_importer.py --update-lookups

# 10. Verify results in Salesforce org
```

### Daily/Weekly Backup Workflow

```bash
# Activate environment
source venv/bin/activate

# Export current data as backup
python3 salesforce_exporter.py

# Store exported_data/ folder in version control or backup location
```

## Support

For issues and questions:

1. Check the console output for detailed error messages
2. Verify your CSV data format and field names
3. Ensure proper Salesforce permissions
4. Review the generated mapping files for data integrity

## Contributing

When modifying the code:

- Maintain the two-phase import pattern
- Update error handling for new scenarios
- Test with various data volumes
- Document any new configuration options
