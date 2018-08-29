import json

def mapConfigDicts(config):
    result = {}
    for k,v in config.__dict__.items():
        try:
            result[k] = v[0]
        except KeyError:
            result[k] = v
    return result
    
class AutoBackupConfig():
    def __init__(self, flags, remoteSql, localSql, email, files):
        self.flags = flags
        self.remoteSql = remoteSql
        self.localSql = localSql
        self.email = email
        self.files = files

    def getDefault():
        flags = {
        "backupToLocalSqlServer" : True,
        "notifyIfBackupFailure" : False,
        "isConfigured" : False,
        "standaloneIndex":True,
        "mysqlIndex":True
        },
        remoteSql = {
        "user":"admin",
        "pass":"1234",
        "host":"example.com",
        "database":"backup_target"
        },
        localSql = {
        "user":"admin",
        "pass":"1234",
        "host":"localhost",
        "database":"backup",
        "indexTable":"autoBackupIndex"
        },
        email = {
        "to": "admin@example.com",
        "from": "alerts@example.com",
        "smtp": "mail.example.com",
        "pass": "1234",
        "subject":"AutoBackup critical failure",
        "message":"dev.stybl.net_autobackup has crashed. Check logs for more information."
        },
        files = {
        "programDirectory":"autoBackup",
        "backupDirectory":"backups",
        "indexFile":"index.db"
        }
        return mapConfigDicts(AutoBackupConfig(flags, remoteSql, localSql, email, files))

    def load(filepath):
        with open(filepath, "r") as file:
            data = json.load(file)
        return AutoBackupConfig(data["flags"], data["remoteSql"], data["localSql"], data["email"], data["files"])
