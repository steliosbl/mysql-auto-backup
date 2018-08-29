import mysql.connector as sql
import subprocess
from subprocess import Popen
import os
import shutil
import logging
import datetime
import sys
import smtplib
import email

if not os.path.isdir("autoBackup"):
    os.mkdir("autoBackup")
os.chdir("autoBackup")

logging.basicConfig(filename='autoBackup.log',
                    level=logging.DEBUG,
                    format='[%(asctime)s] [%(levelname)s]: %(message)s')

#Checks if returned error stream from processes contains something other than ignored warning
def noErrors(err):
    #This warning is ignored for both processes
    err = str(err)
    ignore = ["[Warning] Using a password on the command line interface can be insecure.\n", "mysqldump: ", "mysql: "]
    for i in ignore:
        err = err.replace(i, "")
    return len(err) == 0


def mail(sub, msg):
    email_conf = {
        "to":"sty@stybl.net",
        "from":"alerts@stybl.net",
        "smtp":"mail.stybl.net",
        "pass":"Uoa175z#"
        }
    em = email.message.EmailMessage()
    em.set_content(msg)
    em["Subject"] = sub
    em["From"] = email_conf["from"]
    em["To"] = email_conf["to"]
    m = smtplib.SMTP_SSL(email_conf["smtp"], 465)
    m.login(email_conf["from"], email_conf["pass"])
    m.sendmail(email_conf["from"], email_conf["to"], em.as_string())
    m.quit()


def abort(e):
    logging.critical("Aborting backup")
    mail("A-Server AutoBackup critical failure", "dev.stybl.net_autobackup has crashed. Check logs for more information. \n Traceback follows: \n \n \n {}".format(repr(e)))
    sys.exit(1)
   
logging.info("Initializing backup")
start = datetime.datetime.now()
#Credentials for local and remote MySQL servers
sqlConfig = {
    "user":"backup",
    "pass":"655f755d9e2c8f00f598498cbd6f5451617*6c5962568932e*02c2f08ee80f2c20*e2398652695c6*7161545f6dbc894895f00f8c2e9d6557f556*3ec521ebc3*76c11-f969-9d*4-636*-ebecbf3825c80*2962d1-cbf*-**54-e8f3",
    "host":"clouddb3.papaki.gr",
    "db":"stybl"
    }

sqlLocal = {
    "user":"backup",
    "pass":"ee80f2c20*e2398652695c6*7161545f6dbc894895f00f8c2e9d6557f556*3ec521ebc3*76c11-f969-9d*4-636*",
    "host":"localhost",
    "db":"autoBackup_latest"
    }


try:
    #Run mysqldump command in shell, pipe output to temp.sql file
    logging.info("Dumping remote database")
    command = "mysqldump --skip-comments --extended-insert -h {} -u {} -p'{}' {} > temp.sql".format(sqlConfig["host"], sqlConfig["user"], sqlConfig["pass"], sqlConfig["db"])
    proc = Popen(command, shell="true", stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    output, error = proc.communicate()
    error = error.decode('utf-8')
    if len(output) != 0:
        logging.info("Dump yielded output: {}".format(str(output)))
    if not noErrors(error):
        logging.error("Dump yielded error: {}".format(str(error)))
        abort()
    logging.info("Dump successfully exported to temp.sql")


    #Connect to local server and drop the existing backup
    try:
        logging.info("Connecting to local database")
        conn = sql.connect(user=sqlLocal["user"], password=sqlLocal["pass"], host=sqlLocal["host"], database=sqlLocal["db"])
        cur = conn.cursor()
        logging.info("Deleting previous backup")
        cur.execute("DROP DATABASE {};".format(sqlLocal["db"]))
        cur.execute("CREATE DATABASE {};".format(sqlLocal["db"]))
        cur.execute("USE {};".format(sqlLocal["db"]))
        continueWithLocalBackup = True
    except Exception as e:
        logging.warning("Local database drop yielded exception: " + repr(e))
        continueWithLocalBackup = False
        

    #Run mysql command in shell and use the temp.sql file as source, thus loading the latest backup into the local server
    if continueWithLocalBackup:
        logging.info("Importing dump to local server")
        command = "mysql {} -u {} -p'{}'".format(sqlLocal["db"], sqlLocal["user"], sqlLocal["pass"])
        with open("temp.sql", "r") as input_file:
            proc = Popen(command, shell="true", stdin = input_file, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            output, error = proc.communicate()
            error = error.decode('utf-8')
            if len(output) != 0:
                logging.info("Import yielded output: {}".format(str(output)))
            if not noErrors(error):
                logging.warning("Import yielded error: {}".format(str(error)))


    #Create backups folder if it does not exist
    if not os.path.isdir("backups"):
        logging.info("Creating backups directory")
        os.mkdir("backups")

    #Rename temp.sql and move it to the backups directory
    logging.info("Moving sql file to backups directory")
    q = False
    i = 0
    while not q:
        filename = "backups/completeBackup__{}__{}.sql".format(str(datetime.datetime.now().date()), i)
        if os.path.exists(filename):
            i += 1
        else:
            q = True
    shutil.copyfile("temp.sql", filename)
    os.remove("temp.sql")
    logging.info("Backup filed as: {}".format(filename))


    #Check if index.log exists and has correact header (and therefore correct format)
    valid_header = "#dev.stybl.net_autoBackup::index.log\n\n"
    log_filename = "backups/index.log"
    create_log = False
    if not os.path.exists(log_filename):
        create_log = True
    else:
        with open(log_filename, "r") as file:
            if file.readline() != valid_header:
                create_log = True
    if create_log:
        logging.info("Creating new index file")
        with open(log_filename, "w") as file:
            file.write(valid_header)
            file.write("%-50s %-30s \n \n" % ("FILENAME", "TIMESTAMP"))

    #Write latest entry to log
    logging.info("Adding latest backup to index")
    with open(log_filename, "a") as file:
        file.write("%-50s %-30s \n" % (filename, str(datetime.datetime.now())))

    end = datetime.datetime.now()
    ms = (end-start).seconds * 1000 + (end-start).microseconds / 1000
    logging.info("Backup complete. Process took: {}ms".format(ms))
except Exception as e:
    abort(e)