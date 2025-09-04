import boto3
import hashlib
import os

# ---------------- AWS CONFIG ----------------
S3_BUCKET = 'your-bucket-name'  # Replace with your bucket name
SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:764757150814:s3DuplicateDeletionAlert'

s3 = boto3.client('s3')
sns = boto3.client('sns')

# ---------------- HELPER FUNCTIONS ----------------
def get_s3_object_hash(bucket, key):
    """
    Compute MD5 hash of an S3 object.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read()
    return hashlib.md5(data).hexdigest()

def send_sns_notification(subject, message):
    """
    Send SNS notification.
    """
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )

# ---------------- LAMBDA HANDLER ----------------
def lambda_handler(event, context):
    try:
        # Step 1: List all objects in the bucket
        response = s3.list_objects_v2(Bucket=S3_BUCKET)
        if 'Contents' not in response:
            print("No objects in bucket")
            return
        
        objects = response['Contents']
        hash_map = {}  # hash -> key

        deleted_files = []

        # Step 2: Compute hash for each object and detect duplicates
        for obj in objects:
            key = obj['Key']
            file_hash = get_s3_object_hash(S3_BUCKET, key)

            if file_hash in hash_map:
                # Duplicate found, delete this object
                s3.delete_object(Bucket=S3_BUCKET, Key=key)
                deleted_files.append(key)
                print(f"Deleted duplicate: {key}")
            else:
                hash_map[file_hash] = key

        # Step 3: Send SNS notification if duplicates were deleted
        if deleted_files:
            message = "Deleted duplicate files:\n" + "\n".join(deleted_files)
            send_sns_notification("S3 Duplicate Deletion Alert", message)
            print("SNS notification sent.")
        else:
            print("No duplicates found.")

    except Exception as e:
        print(f"Error: {str(e)}")
        send_sns_notification("S3 Duplicate Deletion Error", str(e))
