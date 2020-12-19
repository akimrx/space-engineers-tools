import os

BASEDIR = r"C:\Users\Administrator\AppData\Roaming\SpaceEngineersDedicated\"
WORLD_NAME = os.environ.get("SE_WORLD_NAME") or r"Star System"
SAVES = os.environ.get("SE_SAVES_PATH") or r"{0}\Saves\{1}\Backup".format(BASEDIR, WORLD_NAME)
PREFIX =  "backups"
BUCKET = "space-engineers"
ENDPOINT = "https://storage.yandexcloud.net"
REGION = "us-east-1"
