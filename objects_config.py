# Salesforce object configuration for export and import
# This list defines the objects to be processed and their import order

# Objects to export/import
# Order matters for import due to lookup relationships
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
