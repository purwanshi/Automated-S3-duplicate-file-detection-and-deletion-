# Automated-S3-duplicate-file-detection-and-deletion-
#1.
Detected duplicate files in an S3 bucket.
#2.
Deleted duplicates automatically using a Lambda function.
#3.Sent notifications via SNS for any deletions.
#4.Used CloudWatch for scheduling and monitoring.
#5.The project was in Python using Boto3 to interact with AWS services:-
-Listing all objects in an S3 bucket.
-Computing hashes of files to detect duplicates.
-Deleting duplicates automatically.
-Sending SNS notifications for any action.
