import pandas as pd
import boto3
import os

# Set up AWS S3 and DynamoDB connections
s3 = boto3.client('s3', region_name='us-east-1')  # Adjust the region if necessary
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')  # Adjust the region if necessary
table = dynamodb.Table('google-emails')  # Your DynamoDB table name

# Function to download a file from S3
def download_file_from_s3(bucket_name, file_key, download_path):
    try:
        s3.download_file(bucket_name, file_key, download_path)
        print(f"Downloaded {file_key} from S3 to {download_path}")
    except Exception as e:
        print(f"Error downloading {file_key}: {e}")

# Function to upload CSV data to DynamoDB
def upload_csv_to_dynamodb(csv_file):
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        # Prepare the item to put in DynamoDB
        item = {
            'ClientID': row['Email'],  # Change this according to your CSV column names
            'Email': row['Email'],
            'Subject': row['Subject'],  # Adjust column names based on your CSV
            'Date': row['Date'],
            'MessageContent': row['MessageContent'],  # Adjust as needed
            'HasAttachment': row['HasAttachment'],  # Adjust as needed
            'AttachmentFilenames': row['AttachmentFilenames']  # Adjust as needed
        }

        # Store the item in DynamoDB
        try:
            table.put_item(Item=item)
            print(f"Uploaded item: {item}")
        except Exception as e:
            print(f"Failed to upload item: {item}. Error: {e}")

# Main execution
if __name__ == '__main__':
    bucket_name = 'google-takeout-emails'  # Your S3 bucket name
    csv_file_key = 'messages.csv'  # Your CSV filename
    local_csv_file_path = '/tmp/messages.csv'  # Temporary path to store downloaded CSV

    # Download the CSV file from S3
    download_file_from_s3(bucket_name, csv_file_key, local_csv_file_path)

    # Upload CSV data to DynamoDB
    upload_csv_to_dynamodb(local_csv_file_path)

    # Optionally, clean up the downloaded file
    os.remove(local_csv_file_path)
