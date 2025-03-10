import boto3
import os
from botocore.exceptions import NoCredentialsError

def upload_to_yandex_s3(local_directory: str, bucket_name: str, s3_folder: str, access_key: str, secret_key: str):
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    try:
        for root, _, files in os.walk(local_directory):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, local_directory)
                s3_path = f"{s3_folder}/{relative_path}".replace("\\", "/")
                
                print(f"Uploading {local_path} to s3://{bucket_name}/{s3_path}")
                s3.upload_file(local_path, bucket_name, s3_path)
                
        print("DONE")
    except NoCredentialsError:
        print("err: wrong data")
    except Exception as e:
        print(f"err: {e}")

if __name__ == "__main__":
    upload_to_yandex_s3(
        local_directory="/path/to/local/files",  
        bucket_name="your-bucket-name",        
        s3_folder="backup",                     
        access_key="your-access-key",         
        secret_key="your-secret-key"           
    )
