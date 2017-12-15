

import socket
import re
import sys
import time
import sqlite3 as db


from connect import Connect


class LockServer(Connect):

    def __init__(self, port_use=None):
        Connect.__init__(self, port_use, self.handler)
        self.create_table()

    def handler(self, message, con, addr):
        if re.match("LOCK_FILE: [a-zA-Z0-9_./]*\nTime: [0-9]*\n\n", message):
            self.get_lock(con, addr, message)
        elif re.match("UNLOCK_FILE: [a-zA-Z0-9_./]*\n\n", message):
            self.get_unlock(con, addr, message)
        else:
            return False
        return True

    def get_lock(self, con, addr, text):
        # Handler for file locking requests
        request_lock = text.splitlines()
        full_path = request_lock[0].split()[1]
        duration_time = int(request_lock[1].split()[1])
        lock_time_info = self.lock_file(full_path, duration_time)
        if lock_time_info:
            return_string = "LOCK_RESPONSE: \nFILENAME: %s\nTIME: %d\n\n" % (full_path, lock_time_info)
        else:
            return_string = "ERROR: %d\nMESSAGE: %s\n\n" % (0, str(duration_time))
        con.sendall(return_string)
        return

    def get_unlock(self, con, addr, text):
        # Handler for file unlocking requests
        request_unlock = text.splitlines()
        full_path = request_unlock[0].split()[1]
        unlock_time = self.unlock_file(full_path)
        return_string_info = "LOCK_RESPONSE: \nFILENAME: %s\nTIME: %d\n\n" % (full_path, unlock_time)
        con.sendall(return_string_info)
        return

    def lock_file(self, path, lock_period):
        # Function that attempts to lock a file
        return_time = -1
        con_lock = db.connect('Database/locking.db')
        # Exclusive r/w access to the db
        con_lock.isolation_level = 'EXCLUSIVE'
        con_lock.execute('BEGIN EXCLUSIVE')
        current_time_info = int(time.time())
        end_time = current_time_info + lock_period
        cur_lock = con_lock.cursor()
        cur_lock.execute("SELECT count(*) FROM Locks WHERE Path = ? AND Time > ?", (path, current_time_info))
        count = cur_lock.fetchone()[0]
        if count is 0:
            cur_lock.execute("INSERT INTO Locks (Path, Time) VALUES (?, ?)", (path, end_time))
            return_time = end_time
        else:
            return_time = False
        # End Exclusive access to the db
        con_lock.commit()
        con_lock.close()
        return return_time

    def unlock_file(self, path):
        # Function that attempts to unlock a file
        return_time = -1
        con_unlock = db.connect('Database/locking.db')
        # Exclusive r/w access to the db
        con_unlock.isolation_level = 'EXCLUSIVE'
        con_unlock.execute('BEGIN EXCLUSIVE')
        current_time_info = int(time.time())
        cur = con_unlock.cursor()
        cur.execute("SELECT count(*) FROM Locks WHERE Path = ? AND Time > ?", (path, current_time_info))
        count = cur.fetchone()[0]
        if count is 0:
            cur.execute("UPDATE Locks SET Time=? WHERE Path = ? AND Time > ?", (current_time_info, path, current_time_info))
        # End Exclusive access to the db
        con_unlock.commit()
        con_unlock.close()
        return current_time_info

    def create_table(cls):
        # Function that creates the tables for the locking servers database
        con_table = db.connect('Database/locking.db')
        with con_table:
            cur = con_table.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS Locks(Id INTEGER PRIMARY KEY, Path TEXT, Time INT)")
            cur.execute("CREATE INDEX IF NOT EXISTS PATHS ON Locks(Path)")


def main():
    try:
        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            port_lock_num = int(sys.argv[1])
            server_info = LockServer(port_lock_num)
        else:
            server_info = LockServer()
        server_info.listen()
    except socket.error, msg:
        print "Unable to create socket connection: " + str(msg)
        con = None

if __name__ == "__main__": main()
