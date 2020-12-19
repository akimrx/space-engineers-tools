#!/usr/bin/env python3

import os
import boto3
import logging

from app.models.base import Base

logger = logging.getLogger(__name__)

ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
SECRET_KEY = os.environ.get('AWS_SECRET_KEY')


class ObjectStorage(Base):
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
        logger.debug(f"Init boto3 session details={self}")
        s3 = boto3.session.Session()
        session = s3.client(
            service_name=self.service,
            endpoint_url=self.endpoint,
        )
        logger.debug(f"Session ready details={session}")
        return session

    async def list_objects(self, prefix: str = None, limit: int = 100):
        objects = self._session().list_objects(
            Bucket=self.bucket,
            MaxKeys=limit,
            Prefix=prefix
        ).get("Contents")

        if not objects:
            return []
        return objects

    def get_object(self, key: str):
        return self._session().get_object(
            Bucket=self.bucket,
            Key=key
        )

    async def upload_object(self, filename: str, prefix: str = None, objectname: str = None):
        obj = f"{prefix}/{objectname}" if prefix else objectname
        logger.debug(f"Uploading file to S3 file={filename} prefix={prefix} name={objectname}")
        self._session().upload_file(filename, self.bucket, obj)
        return self.get_object(key=obj)


