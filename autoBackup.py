from autoBackupConfig import AutoBackupConfig
import mysql.connector as sql
import email
import smtplib
import sys
import subprocess
from subprocess import Popen
import shutil
import os
from autoBackupStandaloneIndex import AutoBackupStandaloneIndex
from autoBackupMysqlIndex import AutoBackupMySQLIndex
import datetime
import json
from pathlib import Path

CONFIG_FILENAME = "config.json"

class AutoBackup:
    def __init__(self, config):
        self.config = config
    
    def filterErrorStream(self, err):
        """Checks error streams from processes to see if they contain any errors other than the known warning"""
        err = str(err)
        ignore = ["[Warning] Using a password on the command line interface can be insecure.\n", "mysqldump: ", "mysql: "]
        for i in ignore:
            err = err.replace(i, "")
        return len(err) == 0

    def sendMail(self, subject, message):
        mail = email.message.EmailMessage()
        mail.set_content(message)
        mail["Subject"] = subject
        mail["From"] = self.config.email["from"]
        mail["To"] = self.config.email["to"]
        client = smtplib.SMTP_SSL(self.config.email["smtp"], 465)
        client.login(self.config.email["from"], self.config.email["pass"])
        client.sendmail(self.config.email["from"], self.config.email["to"], em.as_string())
        client.quit()

    def abort(self, message):
        if self.config.flags["notifyIfBackupFailure"]:
            self.sendMail(self.config.email["subject"], "{} \n Traceback follows: \n \n \n {}".format(self.config.email["message"], repr(e)))
        sys.exit(1)

    def dumpRemoteDatabase(self):
        """Run mysqldump command in shell and pipe output to temp.sql file"""
        command = "mysqldump --skip-comments --extended-insert -h {} -u {} -p'{}' {} > temp.sql".format(self.config.remoteSql["host"], self.config.remoteSql["user"], self.config.remoteSql["pass"], self.config.remoteSql["database"])
        proc = Popen(command, shell="true", stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        output, error = proc.communicate()
        error = error.decode('utf-8')
        return output, error

    def dropLatestFromLocal(self):
        conn = sql.connect(user=self.config.localSql["user"], password=self.config.localSql["pass"], host=self.config.localSql["host"], database=self.config.localSql["database"])
        cur = conn.cursor()
        cur.execute("DROP DATABASE {};".format(self.config.localSql["database"]))
        cur.execute("CREATE DATABASE {};".format(self.config.localSql["database"]))
        conn.commit()
        conn.close()

    def loadTempToLocal(self):
        command = "mysql {} -u {} -p'{}'".format(self.config.localSql["database"], self.config.localSql["user"], self.config.localSql["pass"])
        with open("temp.sql", "r") as input_file:
            proc = Popen(command, shell="true", stdin = input_file, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            output, error = proc.communicate()
        error = error.decode('utf-8')
        return output, error

    def fileBackup(self):
        q = False
        i = 0
        while not q:
            filename = BACKUP_DIR/"completeBackup__{}__{}.sql".format(str(datetime.datetime.now().date()), i)
            if filename.exists():
                i += 1
            else:
                q = True
        shutil.copyfile("temp.sql", str(filename))
        os.remove("temp.sql")
        return filename

    def insertIndex(self, filename, time):
        indexes = []
        if self.config.flags["standaloneIndex"]:
            indexes.append(AutoBackupStandaloneIndex(BACKUP_DIR/self.config.files["indexFile"]))
        if self.config.flags["mysqlIndex"]:
            indexes.append(AutoBackupMySQLIndex(self.config.localSql))
        for i in indexes:
            i.createTable()
            i.insert(filename, time)
            i.close()

    def execute(self):
        startTime = datetime.datetime.now()
        self.dumpRemoteDatabase()
        dumpTime = datetime.datetime.now()
        if self.config.flags["backupToLocalSqlServer"]:
            self.dropLatestFromLocal()
            self.loadTempToLocal()
        filename = self.fileBackup().name
        self.insertIndex(filename, dumpTime)
        endTime = datetime.datetime.now()
        ms = (endTime - startTime).seconds * 1000 + (endTime - startTime).microseconds / 1000

if not os.path.exists(CONFIG_FILENAME):
    with open(CONFIG_FILENAME, "w") as file:
        json.dump(AutoBackupConfig.getDefault().__dict__, file)
else:
    config = AutoBackupConfig.load(CONFIG_FILENAME)
    if config.flags["isConfigured"]:
        PROGRAM_DIR = Path(config.files["programDirectory"])
        BACKUP_DIR = PROGRAM_DIR/config.files["backupDirectory"]
        if not PROGRAM_DIR.exists():
            PROGRAM_DIR.mkdir(parents=True, exist_ok=True)
            BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        else:
            if not BACKUP_DIR.exists():
                BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        auto = AutoBackup(config)
        auto.execute()

    