import boto3

def s3_send_file(filename, bucked, new_name):
    
    """
    *** TO DO ***

    Send a file to AWS s3 bucket using boto3 client.
    Not implemented yet
    """

    s3 = boto3.client("s3")
    
    s3.upload_file(filename, bucked, new_name)


