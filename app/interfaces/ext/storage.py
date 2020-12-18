#!/usr/bin/env python3

import os
import boto3


ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
SECRET_KEY = os.environ.get('AWS_SECRET_KEY')


class ObjectStorage(object):
    def __init__(
        self,
        access_key=None,
        secret_key=None,
        service="s3",
        endpoint="https://storage.yandexcloud.net",
        bucket="space-engineers"
    ):

        self.access_key=access_key or ACCESS_KEY
        self.secret_key=secret_key or SECRET_KEY
        self.service = service
        self.endpoint = endpoint
        self.bucket = bucket


    def _session(self):
        s3 = boto3.session.Session()
        session = s3.client(
            service_name=self.service,
            endpoint_url=self.endpoint,
        )
        return session

    def list_objects(self, path: str = None):
        resource = self.bucket + path if path else self.bucket
        objects = self._session().list_objects(Bucket=resource).get("Contents")

        if not objects:
            return []
        return objects

    def get_object(self, key: str):
        return self._session().get_object(
            Bucket=self.bucket,
            Key=key
        )

    def upload_object(self, filename: str, path: str = None, objectname: str = None):
        obj = f"{path}/{objectname}" if path else objectname
        self._session().upload_file(filename, self.bucket, obj)
        return self.get_object(key=obj)


