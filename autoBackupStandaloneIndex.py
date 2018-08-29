import sqlite3

class AutoBackupStandaloneIndex:
    def __init__(self, location):
        self.location = location
        self.conn = sqlite3.connect(self.location)
        self.db = self.conn.cursor()

    def createTable(self):
        self.db.execute('CREATE TABLE autoBackupIndex (filename TEXT, timestamp TEXT);')
        self.conn.commit()

    def insert(self, filename, time):
        self.db.execute('INSERT INTO autoBackupIndex VALUES (?,?);', (str(filename), str(time)))
        self.conn.commit()

    def close(self):
        self.conn.close()
