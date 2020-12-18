import os

from app.models.backup import Backup

from app.interfaces.ext.storage import ObjectStorage
from zipfile import ZipFile, ZIP_DEFLATED


backup_path = '/Users/faskhutdinov/boto3-test'


def create_archive(path: str, archive_name: str):
    with ZipFile(archive_name, "w", compression=ZIP_DEFLATED) as archive:
        for dirname, subdirs, files in os.walk(path):
            for filename in files:
                fullpath = os.path.join(dirname, filename)
                archive.write(fullpath, os.path.basename(fullpath))
    archive.close()


def upload_backups():
    s3 = ObjectStorage()

    for dirname in os.listdir(backup_path):
        directory_abs_path = f"{backup_path}/{dirname}"
        archive_name = f"{dirname.replace(' ', '-')}.zip"
        archive_abs_path = f"{backup_path}/{archive_name}"

        zipfile = create_archive(directory_abs_path, archive_abs_path)
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
    list_backups()
