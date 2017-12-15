
import socket
import re
import sys
import os
import hashlib
import random
import sqlite3 as db

from connect import Connect


class DirectoryServer(Connect):

    def get_slaves(self, con, addr, text):
        # Function that gets the list of slave servers
        request_info = text.splitlines()
        host = request_info[0].split()[1]
        port = request_info[1].split()[1]
        slave_string_com = self.get_slave_string(host, port)
        return_string_com = "SLAVES: %s\n\n" % slave_string_com
        con.sendall(return_string_com)
        return

    def find_host(self, path):
        # Function that takes a path and returns the server that contains that directories files
        return_host_info = (False, False)
        con_req = db.connect("Database/directories.db")
        with con_req:
            cur_req = con_req.cursor()
            cur_req.execute("SELECT Server FROM Directories WHERE Path = ?", (path,))
            server = cur_req.fetchone()
            if server:
                server_id = server[0]
                cur_req = con_req.cursor()
                cur_req.execute("SELECT Server, Port FROM Servers WHERE Id = ?", (server_id,))
                return_host_info = cur_req.fetchone()
        return return_host_info

    def __init__(self, port_use=None):
        Connect.__init__(self, port_use, self.handler)
        self.create_tables()

    def handler(self, message, con, addr):
        if re.match("GET_SERVER: \nFILENAME: [a-zA-Z0-9_./]*\n\n", message):
            self.get_server(con, addr, message)
        elif re.match("GET_SERVER: \nFILENAME: [a-zA-Z0-9_./]*\n\n", message):
            self.get_slaves(con, addr, message)
        else:
            return False
        return True

    def get_server(self, con, addr, text):
        # Handler for file upload requests
        request_info = text.splitlines()
        full_path = request_info[1].split()[1]
        path, file = os.path.split(full_path)
        name, ext = os.path.splitext(file)
        filename_info = hashlib.sha256(full_path).hexdigest() + ext
        host, port = self.find_host(path)
        if not host:
            # The Directory doesn't exist and must be added to the db
            server_id = self.pick_random_host()
            self.create_dir(path, server_id)
            host, port = self.find_host(path)
        # Get the list of slaves that have a copy of the file
        slave_string_info = self.get_slave_string(host, port)
        return_string_info = "PRIMARY_SERVER: %s\nPORT: %s\nFILENAME: %s%s\n\n" % (host, port, filename_info, slave_string_info)
        print return_string_info
        con.sendall(return_string_info)
        return

    def pick_random_host(self):
        # Function to pick a random host from the database
        return_host_info = False
        con_req = db.connect("Database/directories.db")
        with con_req:
            cur_req = con_req.cursor()
            cur_req.execute("SELECT Id FROM Servers")
            servers = cur_req.fetchall()
            if servers:
                return_host_info = random.choice(servers)[0]
        return return_host_info

    def get_slave_string(self, host, port):
        # Function that generates a slave string
        return_string_info = ""
        con_req = db.connect("Database/directories.db")
        with con_req:
            cur_req = con_req.cursor()
            cur_req.execute("SELECT Server, Port FROM Servers WHERE NOT (Server=? AND Port=?)", (host, port,))
            servers = cur_req.fetchall()
        for (host, port) in servers:
            header = "\nSLAVE_SERVER: %s\nPORT: %s" % (host, port)
            return_string_info = return_string_info + header
        return return_string_info

    def create_dir(self, path, host):
        # Function to create a directory in the DB
        con_req = db.connect("Database/directories.db")
        with con_req:
            cur_req = con_req.cursor()
            cur_req.execute("INSERT INTO Directories (Path, Server) VALUES (?, ?)", (path, host,))
        con_req.commit()
        con_req.close()

    def add_server(self, host, port):
        # Function to add a server to the DB
        con_req = db.connect("Database/directories.db")
        with con_req:
            cur_req = con_req.cursor()
            cur_req.execute("INSERT INTO Servers (Server, Port) VALUES (?, ?)", (host, port,))
        con_req.commit()
        con_req.close()

    def remove_dir(self, path):
        # Function to remove a directory from the DB
        con_req = db.connect("Database/directories.db")
        with con_req:
            cur_dir = con_req.cursor()
            cur_dir.execute("DELETE FROM Directories WHERE Path = ?", (path,))
        con_req.commit()
        con_req.close()

    def remove_server(self, server):
        # Function to remove a server from the DB
        con_rem = db.connect("Database/directories.db")
        with con_rem:
            cur_rem = con_rem.cursor()
            cur_rem.execute("DELETE FROM Servers WHERE Server = ?", (server,))
        con_rem.commit()
        con_rem.close()

    def create_tables(self):
        # Function to add the tables to the database
        con_tab = db.connect("Database/directories.db")
        with con_tab:
            cur_tab = con_tab.cursor()
            cur_tab.execute("CREATE TABLE IF NOT EXISTS Servers(Id INTEGER PRIMARY KEY, Server TEXT, Port TEXT)")
            cur_tab.execute("CREATE UNIQUE INDEX IF NOT EXISTS SERVS ON Servers(Server, Port)")
            cur_tab.execute("CREATE TABLE IF NOT EXISTS Directories(Id INTEGER PRIMARY KEY, Path TEXT, Server INTEGER, FOREIGN KEY(Server) REFERENCES Servers(Id))")
            cur_tab.execute("CREATE UNIQUE INDEX IF NOT EXISTS DIRS ON Directories(Path)")

def main():
    try:
        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            port_num = int(sys.argv[1])
            server_dir = DirectoryServer(port_num)
        else:
            server_dir = DirectoryServer()
        server_dir.listen()
    except socket.error, msg:
        print "Unable to create socket connection: " + str(msg)
        con = None


if __name__ == "__main__": main()
