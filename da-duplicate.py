import json
import csv

# Load the data from the JSON file
with open('emails.json', 'r') as file:
    data = json.load(file)

# Extract unique emails
emails_seen = set()
unique_emails = []

for item in data['Items']:
    email = item['Email']['S']  # Adjust this if the structure of your email data is different
    if email not in emails_seen:
        unique_emails.append(item)
        emails_seen.add(email)

# Write the unique emails to a CSV file
with open('unique_emails.csv', 'w', newline='') as csvfile:
    fieldnames = ['Email', 'Timestamp']  # Adjust according to the fields in your table
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for item in unique_emails:
        writer.writerow({
            'Email': item['Email']['S'],  # Adjust to match your field names
            'Timestamp': item['Timestamp']['S']  # Adjust to match your field names
        })

print("CSV file 'unique_emails.csv' created successfully.")
