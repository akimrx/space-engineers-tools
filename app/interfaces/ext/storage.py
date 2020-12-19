#!/usr/bin/env python3

import os
import aioboto3
import logging

from typing import List, Optional
from app.models.base import Base

logger = logging.getLogger(__name__)


class ObjectStorage(Base):
    """Async wrapper for aioboto3 (S3).
    By default credentials and config reading from ~/.aws

    """

    def __init__(
        self,
        region_name=None,
        aws_access_key_id=None,
        aws_access_secret_key=None,
        endpoint=None,
        bucket=None
    ):

        self.access_key = aws_access_key_id or os.environ.get("S3_ACCESS_KEY")
        self.secret_key = aws_access_secret_key or os.environ.get("S3_SECRET_KEY")
        self.region = region_name or os.environ.get("S3_REGION")
        self.endpoint = endpoint or os.environ.get("S3_ENDPOINT")
        self.bucket = bucket or os.environ.get("S3_BUCKET")

    async def list_objects(
        self,
        prefix: str = "",
        limit: int = 100,
        as_url: bool = False,
        simple: bool = False
    ) -> List:

        objects = []
        async with aioboto3.resource("s3", endpoint_url=self.endpoint) as s3:
            bucket = await s3.Bucket(self.bucket)

            async for obj in bucket.objects.filter(MaxKeys=limit, Prefix=prefix):
                if simple and as_url:
                    raise RuntimeError(f"'simple' and 'as_url' args can't be passed together.")
                elif simple:
                    obj = f"{obj.bucket_name}/{obj.key}"
                elif as_url:
                    obj = f"{self.endpoint}/{obj.bucket_name}/{obj.key}"

                objects.append(obj)

        return objects

    async def upload_object(
        self,
        filename: str,
        filepath: str,
        bucket: str = None,
        prefix: str = None,
    ) -> Optional[str]:

        bucket = bucket or self.bucket
        blob_s3_key = f"{prefix}/{filename}" if prefix else filename

        async with aioboto3.client("s3", endpoint_url=self.endpoint) as s3:
            try:
                with open(filepath, "rb") as data:
                    logger.info(f"Uploading {blob_s3_key} to s3")
                    await s3.upload_fileobj(data, bucket, blob_s3_key)
                    logger.info(f"Finished Uploading {blob_s3_key} to s3")

                result = f"{self.endpoint}/{bucket}/{blob_s3_key}"
            except Exception as e:
                logger.error(f"Unable to s3 upload {filepath} to {blob_s3_key}: {e} ({type(e)})")
                result = None

        return result
