import pandas as pd
import boto3
import re

def extract_email_and_name(row):
    # Use regex to find email addresses and names
    email_match = re.findall(r'<([^>]+)>', row[2])
    name_match = re.findall(r'([^<]+)', row[2])
    
    if email_match:
        email = email_match[0]
    else:
        email = None
        
    if name_match:
        name = name_match[0].strip()
    else:
        name = None
    
    return email, name

def process_csv(local_csv_file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(local_csv_file_path, header=None)

    # Create a list to hold processed entries
    processed_entries = []

    for _, row in df.iterrows():
        email, name = extract_email_and_name(row)
        
        # Check for attachment presence
        has_attachment = 'attachments' in row[5].lower() if len(row) > 5 else False
        
        # Extract timestamp from the row
        timestamp = row[3] if len(row) > 3 else None
        
        processed_entries.append({
            'Email': email,
            'Name': name,
            'Attachments': has_attachment,
            'MessageContent': row[5] if len(row) > 5 else None,
            'Timestamp': timestamp
        })

    # Remove duplicates based on email and message content
    processed_entries = pd.DataFrame(processed_entries).drop_duplicates(subset=['Email', 'MessageContent'])

    return processed_entries

def upload_to_dynamodb(processed_entries):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('google-emails')  # Change to your actual table name

    for _, entry in processed_entries.iterrows():
        table.put_item(
            Item={
                'Email': entry['Email'],
                'Name': entry['Name'],
                'Attachments': entry['Attachments'],
                'MessageContent': entry['MessageContent'],
                'Timestamp': entry['Timestamp'],
            }
        )

if __name__ == "__main__":
    local_csv_file_path = '/tmp/messages.csv'  # Update with the actual path where your CSV file is downloaded
    processed_entries = process_csv(local_csv_file_path)
    upload_to_dynamodb(processed_entries)
    print("Data uploaded to DynamoDB successfully.")
