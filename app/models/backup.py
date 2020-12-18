#!/usr/bin/env python3

"""This module contains backup object class."""

from app.models.base import Base
from app.constants import BUCKET, ENDPOINT

class Backup(Base):

    def __init__(
        self,
        Key=None,
        LastModified=None,
        tzinfo=None,
        ETag=None,
        Size=None,
        StorageClass=None,
        Owner=None,
        client=None,
        **kwargs
    ):
        self.key = Key
        self.last_modified = LastModified
        self.tzinfo = tzinfo
        self.etag = ETag
        self.size = Size
        self.storage_class = StorageClass
        self.owner = Owner

        self._client = client
        self._other = kwargs

    @property
    def url(self):
        string = f"https://{ENDPOINT}/{BUCKET}/{self.key.replace(' ','%20')}"
        return string

    @classmethod
    def de_json(cls, data: dict, client: object = None):
        """Packages the dict into an object."""
        if not data:
            return None

        data = super(Backup, cls).de_json(data, client)
        return cls(client=client, **data)

    @classmethod
    def de_list(cls, data: list, client: object = None):
        """Packages each dict in the list into an object."""
        if not data:
            return []

        backups = list()
        for backup in data:
            backups.append(cls.de_json(backup, client))
        return backups

