import boto3
import email
from email import policy
from email.parser import BytesParser
import os

# Set up DynamoDB connection
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')  # Change region as needed
table = dynamodb.Table('ClientEmails')  # Replace with your table name

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
        if filename.endswith('.nmap3'):  # Adjust for file extension
            file_path = os.path.join(directory, filename)
            email_data = parse_email(file_path)
            store_in_dynamodb(email_data)
            print(f"Stored email from {email_data['Sender']}, HasAttachment: {email_data['HasAttachment']}")

# Run scrubber on a specific folder
if __name__ == '__main__':
    scrub_emails('/path/to/your/nmap3/folder')  # Update path
