import os
import asyncio
import logging

from app.models.backup import Backup
from app.constants import SAVES, PREFIX
from app.interfaces.ext.storage import ObjectStorage

from zipfile import ZipFile, ZIP_DEFLATED


logging.basicConfig(
    level=logging.INFO,
    datefmt="%d %b %H:%M:%S",
    format="%(asctime)s – %(levelname)s – %(message)s"
)

# Disable boto3 debug messages
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('nose').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


async def create_archive(path: str, archive_name: str) -> None:
    logger.info(f"Compressing {archive_name}")

    with ZipFile(archive_name, "w", compression=ZIP_DEFLATED) as archive:
        for dirname, subdirs, files in os.walk(path):
            for filename in files:
                fullpath = os.path.join(dirname, filename)
                archive.write(fullpath, os.path.basename(fullpath))
    archive.close()


async def upload_backups() -> None:
    logger.info(f"Prepare backups for upload to S3...")

    for dirname in os.listdir(SAVES):
        directory_abs_path = f"{SAVES}/{dirname}"
        archive_name = f"{dirname.replace(' ', '-')}.zip"
        archive_abs_path = f"{SAVES}/{archive_name}"

        logger.info(f"Prepare .zip archive...")
        await create_archive(directory_abs_path, archive_abs_path)

        logger.info(f"Starting upload file {archive_name}")
        await ObjectStorage().upload_object(
            filename=archive_abs_path,
            prefix=PREFIX,
            objectname=archive_name
        )

        logger.info(f"Removing artifacts: {archive_abs_path}")
        os.remove(archive_abs_path)


async def list_backups():
    data = await ObjectStorage().list_objects(prefix=PREFIX)
    objects = Backup.de_list(data)

    for obj in objects:
        print(obj.url)


def main():
    loop = asyncio.get_event_loop()
    tasks = [upload_backups, list_backups]

    for task in tasks:
        loop.run_until_complete(
            asyncio.wait(
                [task()]
            )
        )

if __name__ == "__main__":
    main()