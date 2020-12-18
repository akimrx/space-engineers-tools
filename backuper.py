import os

from app.models.backup import Backup
from app.constants import SAVES
from app.interfaces.ext.storage import ObjectStorage

from zipfile import ZipFile, ZIP_DEFLATED


def create_archive(path: str, archive_name: str) -> None:
    with ZipFile(archive_name, "w", compression=ZIP_DEFLATED) as archive:
        for dirname, subdirs, files in os.walk(path):
            for filename in files:
                fullpath = os.path.join(dirname, filename)
                archive.write(fullpath, os.path.basename(fullpath))
    archive.close()


def upload_backups() -> None:
    s3 = ObjectStorage()

    for dirname in os.listdir(SAVES):
        directory_abs_path = f"{SAVES}/{dirname}"
        archive_name = f"{dirname.replace(' ', '-')}.zip"
        archive_abs_path = f"{SAVES}/{ia::rchive_name}"

        create_archive(directory_abs_path, archive_abs_path)
        
        s3.upload_object(
            filename=archive_abs_path,
            path="backups",
            objectname=archive_name
        )

        os.remove(archive_abs_path)


def list_backups():
    s3 = ObjectStorage()

    objects = Backup.de_list(s3.list_objects())
    for obj in objects:
        print(obj.url)


if __name__ == "__main__":
    upload_backups()

