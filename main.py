import pandas as pd
import boto3
import os

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('google-emails')  # Change to your DynamoDB table name

def extract_email_and_name(row):
    # Extract the email and name from the appropriate columns
    # Adjust the indices based on your CSV structure
    email = row[2] if len(row) > 2 else None  # Assuming email is in the 3rd column (index 2)
    name = row[1].split('<')[0].strip() if len(row) > 1 else None  # Assuming name is in the 2nd column (index 1)
    return email, name

def process_csv(local_csv_file_path):
    # Read the CSV file into a DataFrame without headers
    df = pd.read_csv(local_csv_file_path, header=None)

    # Create a list to hold processed entries
    processed_entries = []

    for _, row in df.iterrows():
        # Access the columns using numerical indices
        email, name = extract_email_and_name(row)

        # Check for attachment presence (based on your sample)
        has_attachment = (
            'attachments' in row[5].lower() if len(row) > 5 and isinstance(row[5], str) else False
        )

        # Extract timestamp from the row (index 3 based on your sample)
        timestamp = row[3] if len(row) > 3 else None

        processed_entries.append({
            'Email': email,
            'Name': name,
            'Attachments': has_attachment,
            'MessageContent': row[5] if len(row) > 5 else None,
            'Timestamp': timestamp
        })

    # Convert processed entries to a DataFrame and remove duplicates based on email and message content
    processed_entries = pd.DataFrame(processed_entries).drop_duplicates(subset=['Email', 'MessageContent'])

    return processed_entries

def upload_to_dynamodb(processed_entries):
    for index, row in processed_entries.iterrows():
        # Upload each entry to DynamoDB
        table.put_item(Item={
            'Email': row['Email'],
            'Name': row['Name'],
            'Attachments': row['Attachments'],
            'MessageContent': row['MessageContent'],
            'Timestamp': row['Timestamp']
        })

def upload_csv_to_dynamodb(local_csv_file_path):
    processed_entries = process_csv(local_csv_file_path)
    upload_to_dynamodb(processed_entries)

if __name__ == "__main__":
    # Path to your CSV file downloaded from S3
    local_csv_file_path = '/tmp/messages.csv'  # Adjust this path if needed

    # Call the upload function
    upload_csv_to_dynamodb(local_csv_file_path)
