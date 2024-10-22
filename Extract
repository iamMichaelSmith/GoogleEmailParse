import json
import pandas as pd

# Load data from the JSON file
with open('all_emails.json') as f:
    data = json.load(f)

# Assuming the email is stored in 'ClientID' field, adapt if necessary
emails = [item['ClientID'] for item in data['Items'] if 'ClientID' in item]

# Remove duplicates by converting the list to a set
unique_emails = list(set(emails))

# Create a DataFrame and export to CSV
df = pd.DataFrame(unique_emails, columns=['Email'])
df.to_csv('unique_emails.csv', index=False)

print(f"Exported {len(unique_emails)} unique emails to unique_emails.csv")
