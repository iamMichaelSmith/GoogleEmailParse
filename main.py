import boto3
import email
from email import policy
from email.parser import BytesParser
import os

# Set up DynamoDB and S3 connections for us-east-1 region
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')  # Updated region
table = dynamodb.Table('google-emails')  # Your DynamoDB table
s3 = boto3.client('s3', region_name='us-east-1')  # Updated region

# Function to download files from S3 bucket
def download_emails_from_s3(bucket_name, download_directory):
    s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix='temp/')
    for obj in s3_objects.get('Contents', []):
        file_name = obj['Key']
        
        # Extract only the file name (remove 'temp/' prefix)
        local_file_name = file_name.split('/')[-1]  # Get only the last part of the path
        local_file_path = os.path.join(download_directory, local_file_name)

        if not local_file_name:
            print(f"No valid file name found for S3 object {file_name}. Skipping.")
            continue  # Skip if no valid file name
        
        try:
            print(f"Attempting to download {file_name} to {local_file_path}")
            s3.download_file(bucket_name, file_name, local_file_path)
            print(f"Downloaded {local_file_name} from S3")
        except Exception as e:
            print(f"Failed to download {local_file_name}: {e}")

# Function to parse nmap3 email format and check for attachments
def parse_email(file_path):
    with open(file_path, 'rb') as f:
        msg = BytesParser(policy=policy.default).parse(f)
    
    # Extract details
    sender = msg['from']
    subject = msg['subject']
    date = msg['date']
    body = msg.get_body(preferencelist=('plain')).get_content()

    # Initialize attachment flag and list
    has_attachment = False
    attachment_filenames = []

    # Check for attachments
    for part in msg.iter_parts():
        if part.get_content_disposition() == 'attachment':
            has_attachment = True
            attachment_filenames.append(part.get_filename())

    # Return parsed details as a dictionary
    return {
        'Sender': sender,
        'Subject': subject,
        'Date': date,
        'Body': body,
        'HasAttachment': has_attachment,
        'AttachmentFilenames': attachment_filenames
    }

# Function to store email data in DynamoDB
def store_in_dynamodb(email_data):
    table.put_item(
        Item={
            'ClientID': email_data['Sender'],  # Use email as ClientID
            'Email': email_data['Sender'],
            'Subject': email_data['Subject'],
            'Date': email_data['Date'],
            'MessageContent': email_data['Body'],
            'HasAttachment': email_data['HasAttachment'],
            'AttachmentFilenames': email_data['AttachmentFilenames']
        }
    )

# Scrubber function to loop over emails
def scrub_emails(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.nmap3'):  # Adjust for your file extension
            file_path = os.path.join(directory, filename)
            email_data = parse_email(file_path)
            store_in_dynamodb(email_data)
            print(f"Stored email from {email_data['Sender']}, HasAttachment: {email_data['HasAttachment']}")

# Main function to download and scrub emails
if __name__ == '__main__':
    download_directory = '/home/ec2-user/email-downloads'  # Update this path
    os.makedirs(download_directory, exist_ok=True)  # Ensure the download directory exists
    download_emails_from_s3('google-takeout-emails', download_directory)
    scrub_emails(download_directory)
