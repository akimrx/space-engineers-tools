<<<<<<< HEAD
SAVES = "/Users/faskhutdinov/boto3-test" # r"C:\space-engineers\torch-server\Instance\Saves\bad-engineers\Backup"
=======
import os

SAVES = os.environ.get("SE_SAVES_PATH") or r"C:\space-engineers\torch-server\Instance\Saves\bad-engineers\Backup"
PREFIX = "backups"
>>>>>>> d2cac6ce32b1127801a53672bb76fa29c749eb39
BUCKET = "space-engineers"
ENDPOINT = "https://storage.yandexcloud.net"
REGION = "us-east-1"

