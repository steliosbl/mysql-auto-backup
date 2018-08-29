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
import logging

CONFIG_FILENAME = "autoBackupConfig.json"

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
            self.sendMail(self.config.email["subject"], "{} \n Traceback follows: \n \n \n {}".format(self.config.email["message"], str(e)))
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

        try:
            logging.debug("Beginning remote database dump.")
            output, error = self.dumpRemoteDatabase()
            dumpTime = datetime.datetime.now()
            logging.info("Remote database dump complete.")
            if len(output) != 0:
                logging.warning("Dump yielded output: {}".format(str(output)))
            if not self.filterErrorStream(error):
                logging.critical("Dump yielded error: {}".format(str(error)))
                self.abort(error)
        except Exception as e:
            logging.critical("Remote database dump yielded exception: " + repr(e))
            self.abort(repr(e))
        
        if self.config.flags["backupToLocalSqlServer"]:
            try:
                logging.debug("Beginning load to local database.")
                logging.debug("Dropping old backup from local database.")
                self.dropLatestFromLocal()
                logging.debug("Loading new backup into local database.")
                output, error = self.loadTempToLocal()
                logging.info("Load of backup into local database complete.")
                if len(output) != 0:
                    logging.warning("Load to local database yielded output: {}".format(str(output)))
                if not self.filterErrorStream(error):
                    logging.warning("Load to local database yielded error: {}".format(str(error)))
            except Exception as e:
                logging.error("Local database load yielded exception: " + repr(e))

        try:
            logging.debug("Beginning filing proces.s")
            filename = self.fileBackup().name
            logging.info("Backup filed with filename: [{}]".format(filename))
            self.insertIndex(filename, dumpTime)
            logging.info("Latest backup added to index(es).")
        except Exception as e:
            logging.critical("Filing of backup failed: " + repr(e))
            self.abort(repr(e))

        endTime = datetime.datetime.now()
        ms = (endTime - startTime).seconds * 1000 + (endTime - startTime).microseconds / 1000
        logging.info("Backup complete. Process took: {}ms".format(ms))

logging.basicConfig(filename='autoBackup.log',
                    level=logging.DEBUG,
                    format='[%(asctime)s] [%(levelname)s]: %(message)s')

if not os.path.exists(CONFIG_FILENAME):
    with open(CONFIG_FILENAME, "w") as file:
        json.dump(AutoBackupConfig.getDefault().__dict__, file)
        logging.critical("Configuration file not found. Creating new file with default values. Please edit it and set the 'isConfigured' flag to true once finished.")
else:
    config = AutoBackupConfig.load(CONFIG_FILENAME)
    if config.flags["isConfigured"]:
        logging.debug("Configuration loaded")
        PROGRAM_DIR = Path(config.files["programDirectory"])
        BACKUP_DIR = PROGRAM_DIR/config.files["backupDirectory"]
        if not PROGRAM_DIR.exists():
            logging.warning("Expected directory structure not found. Creating missing directories.")
            PROGRAM_DIR.mkdir(parents=True, exist_ok=True)
            BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        else:
            if not BACKUP_DIR.exists():
                logging.warning("Expected directory structure not found. Creating missing directories.")
                BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        auto = AutoBackup(config)
        logging.info("Executing backup")
        auto.execute()
    else:
        logging.critical("Execution aborted because config file marked as unconfigured. Did you forget to set 'isConfigured' to true after editing it?")
    