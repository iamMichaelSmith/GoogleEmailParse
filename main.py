import boto3
import email
from email import policy
from email.parser import BytesParser
import os
from io import BytesIO

# Set up DynamoDB and S3 connections for us-east-1 region
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')  # Updated region
table = dynamodb.Table('google-emails')  # Your DynamoDB table
s3 = boto3.client('s3', region_name='us-east-1')  # Updated region

# Function to read and parse emails directly from S3
def scrub_emails_from_s3(bucket_name):
    # List objects in the root of the bucket
    s3_objects = s3.list_objects_v2(Bucket=bucket_name)
    
    if 'Contents' not in s3_objects:
        print("No objects found in the bucket.")
        return

    for obj in s3_objects['Contents']:
        file_name = obj['Key']

        # Ensure we only process files (not folders)
        if file_name.endswith('.nmap3'):  # Adjust this to the extension you're expecting
            try:
                print(f"Reading {file_name} from S3")
                response = s3.get_object(Bucket=bucket_name, Key=file_name)
                email_content = response['Body'].read()  # Read the file content

                # Parse the email
                email_data = parse_email(BytesIO(email_content))  # Pass the content as a BytesIO stream
                store_in_dynamodb(email_data)  # Store the parsed data in DynamoDB
                print(f"Stored email from {email_data['Sender']}, HasAttachment: {email_data['HasAttachment']}")
            except Exception as e:
                print(f"Failed to read {file_name}: {e}")

# Function to parse nmap3 email format and check for attachments
def parse_email(file_stream):
    msg = BytesParser(policy=policy.default).parse(file_stream)
    
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

# Main function to scrub emails directly from S3
if __name__ == '__main__':
    scrub_emails_from_s3('google-takeout-emails')
