import os
import asyncio

from app.models.backup import Backup
from app.constants import SAVES, PREFIX
from app.interfaces.ext.storage import ObjectStorage

from zipfile import ZipFile, ZIP_DEFLATED


async def create_archive(path: str, archive_name: str) -> None:
    with ZipFile(archive_name, "w", compression=ZIP_DEFLATED) as archive:
        for dirname, subdirs, files in os.walk(path):
            for filename in files:
                fullpath = os.path.join(dirname, filename)
                archive.write(fullpath, os.path.basename(fullpath))
    archive.close()


async def upload_backups() -> None:
    for dirname in os.listdir(SAVES):
        directory_abs_path = f"{SAVES}/{dirname}"
        archive_name = f"{dirname.replace(' ', '-')}.zip"
        archive_abs_path = f"{SAVES}/{archive_name}"

        await create_archive(directory_abs_path, archive_abs_path)

        await ObjectStorage().upload_object(
            filename=archive_abs_path,
            path=PREFIX,
            objectname=archive_name
        )

        os.remove(archive_abs_path)


async def list_backups():
    data = await ObjectStorage().list_objects()
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