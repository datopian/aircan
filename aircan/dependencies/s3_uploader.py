import json
import logging
import boto3

class s3Uploader(object):
    def __init__(self,**arg):
        self.s3 = boto3.client(
            service_name=arg['service_name'],
            aws_access_key_id=arg['aws_access_key_id'],
            aws_secret_access_key=arg['aws_secret_access_key'],
            endpoint_url=arg['endpoint_url'],
            region_name=arg['region_name'],
        )
        self.bucket_name = arg['bucket_name']
    
    def upload_file(self, file_path, file_name):
        self.s3.upload_file(file_path, self.bucket_name, file_name)

    def upload_fileobj(self, fileobj, file_name):
        self.s3.upload_fileobj(fileobj, self.bucket_name, file_name)

    def upload_string(self, string, file_name):
        self.s3.put_object(Body=string, Bucket=self.bucket_name, Key=file_name)

    def upload_json(self, json_obj, file_name):
        self.s3.put_object(Body=json.dumps(json_obj), Bucket=self.bucket_name, Key=file_name)

    def upload_bytes(self, bytes, file_name):
        self.s3.put_object(Body=bytes, Bucket=self.bucket_name, Key=file_name)

    def upload_file_to_s3(self, file_path, file_name, content_type):
        self.s3.upload_file(file_path, self.bucket_name, file_name, ExtraArgs={'ContentType': content_type})

    def upload_fileobj_to_s3(self, fileobj, file_name, content_type):
        self.s3.upload_fileobj(fileobj, self.bucket_name, file_name, ExtraArgs={'ContentType': content_type})

    def upload_string_to_s3(self, string, file_name, content_type):
        self.s3.put_object(Body=string, Bucket=self.bucket_name, Key=file_name, ContentType=content_type)

    def upload_json_to_s3(self, json_obj, file_name, content_type):
        self.s3.put_object(Body=json.dumps(json_obj), Bucket=self.bucket_name, Key=file_name, ContentType=content_type)

    def upload_bytes_to_s3(self, bytes, file_name, content_type):
        self.s3.put_object(Body=bytes, Bucket=self.bucket_name, Key=file_name, ContentType=content_type)