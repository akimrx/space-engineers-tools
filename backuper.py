import os
import asyncio
import logging

from typing import Coroutine
from itertools import groupby
from zipfile import ZipFile, ZIP_DEFLATED

from app.constants import SAVES, PREFIX, ENDPOINT, BUCKET
from app.interfaces.ext.storage import ObjectStorage


logging.basicConfig(
    level=logging.INFO,
    datefmt="%H:%M:%S",
    format="%(asctime)s – %(levelname)s – %(message)s"
)

# Disable boto3 debug messages
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('nose').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
s3 = ObjectStorage(endpoint=ENDPOINT, bucket=BUCKET)


def create_archive(filepath: str, archive_name: str, dest: str = None) -> str:
    if not archive_name.endswith(".zip"):
        archive_name = f"{archive_name}.zip"

    if dest is None:
        dest = os.getcwd()

    blob = f"{dest}/{archive_name}"
    logger.debug(f"Creating {blob}")

    with ZipFile(blob, "w", compression=ZIP_DEFLATED) as archive:
        for dirname, subdirs, files in os.walk(filepath):
            for filename in files:
                fullpath = os.path.join(dirname, filename)
                archive.write(fullpath, os.path.basename(fullpath))
    archive.close()
    return blob


def remove_artifacts(filepath: str) -> None:
    try:
        logger.debug(f"Removing artifacts {filepath}")
        os.remove(filepath)
    except Exception as e:
        logging.error(f"Can't delete artifact {filepath}, cause: {e}")


async def list_online_backups() -> List:
    objects = await s3.list_objects(prefix=PREFIX, simple=True)
    return objects


async def upload_backups(dirname: str) -> None:
    backup_path = f"{SAVES}/{dirname}"
    filename = dirname.replace(' ', '-')
    search_pattern = f"{BUCKET}/{PREFIX}/{filename}.zip"

    existing_objects = await list_online_backups()
    if search_pattern in existing_objects:
        logging.warning(f"File {search_pattern} already uploaded, skipping")
        return

    compressed_backup = create_archive(backup_path, filename, dest=SAVES)
    upload_result = await s3.upload_object(
        filename=compressed_backup.split("/")[-1],
        filepath=compressed_backup,
        prefix=PREFIX
    )

    if upload_result is not None:
        remove_artifacts(compressed_backup)
    else:
        logger.error(f"File {compressed_backup} not uploaded!")
        logger.warning(f"Artifact {compressed_backup} was not deleted")


async def print_online_backups(group_by_date: bool = True) -> None:
    objects = await s3.list_objects(prefix=PREFIX, as_url=True)
    print("\n")
    print("=" * 77)
    print(" " * 28, "ONLINE BACKUPS")
    print("=" * 77)

    if group_by_date:
        grouped_list = [
            list(j) for i, j in groupby(objects, lambda k: "-".join(k.split("/")[-1].split("-")[:3]))
        ]
        for backup_group in grouped_list:
            print("-" * 77)
            print(" " * 30, "/".join(backup_group[0].split("/")[-1].split("-")[:3]))
            print("-" * 77)
            for backup in backup_group:
                print(backup)

    else:
        for i in reversed(objects):
            print(i)


def generate_upload_tasks() -> Coroutine:
    tasks = [
        upload_backups(dirname)
        for dirname in os.listdir(SAVES)
    ]

    logger.info(f"Create {len(tasks)} tasks for uploading.")
    return asyncio.gather(*tasks)


def main(tasks) -> None:
    loop = asyncio.get_event_loop()

    for task in tasks:
        loop.run_until_complete(task())
    loop.close()


if __name__ == "__main__":
    logger.info(f"Found {len(os.listdir(SAVES))} backups.")
    tasks = [generate_upload_tasks, print_online_backups]
    main(tasks)
