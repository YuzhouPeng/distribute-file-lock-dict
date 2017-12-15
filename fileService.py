
import socket
import os
import re
import sys
import base64
import connect
from connect import Connect


class FileServer(Connect):

    File_location = os.path.join(os.getcwd(), "DirectoryServerFiles")
    def __init__(self, port_use=None):
        Connect.__init__(self, port_use, self.handler)
        self.File_location = os.path.join(self.File_location, str(self.Port_num))

    def handler(self, message, con, addr):
        if re.match("UPLOAD: [a-zA-Z0-9_.]*\nDATA: .*\n\n", message):
            self.upload(con, addr, message)
        elif re.match("DOWNLOAD: [a-zA-Z0-9_.]*\n\n", message):
            self.download(con, addr, message)
        elif re.match("UPDATE: [a-zA-Z0-9_.]*\nDATA: .*\n\n", message):
            self.update(con, addr, message)
        else:
            return False

        return True

    def upload(self, con, addr, text):
        # Handler for file upload requests
        filename, data = self.execute_write(text)
        return_string_rep = "OK: 0\n\n"
        con.sendall(return_string_rep)
        self.update_slaves(filename, data)
        return

    def download(self, con, addr, text):
        # Handler for file download requests
        request_info = text.splitlines()
        filename = request_info[0].split()[1]
        path_info = os.path.join(self.File_location, filename)
        file_handle = open(path_info, "w+")
        data = file_handle.read()
        return_string_info = "DATA: %s\n\n" % (base64.b64encode(data))
        con.sendall(return_string_info)
        return

    def update(self, con, addr, text):
        # Handler for file update requests
        self.execute_write(text)
        return_string_info = "OK: 0\n\n"
        con.sendall(return_string_info)
        return

    def execute_write(self, text):
        # Function that process an update/upload request and writes data to the server
        request = text.splitlines()
        filename_w = request[0].split()[1]
        data = request[1].split()[1]
        data = base64.b64decode(data)
        path_info = os.path.join(self.File_location, filename_w)
        file_handle = open(path_info, "w+")
        file_handle.write(data)
        return filename_w, data


    def update_slaves(self, filename, data):
        # Function that gets all the slaves and updates file on them
        slaves = self.get_slaves()
        update_s = "UPDATE: %s\nDATA: %s\n\n" % (filename, base64.b64encode(data))
        for (host, port) in slaves:
            self.send_request(update_s, host, int(port))
        return

    def get_slaves(self):
        # Function to get the list of slave file servers
        slave_return_list = []
        request_data = "GET_SLAVES: %s\nPORT: %s\n\n" % (self.Host_num, self.Port_num,)
        lines_req = self.send_request(request_data, "0.0.0.0", 7333).splitlines()
        slaves = lines_req[1:-1]
        for i in range(0, len(slaves), 2):
            host = slaves[i].split()[1]
            port = slaves[i + 1].split()[1]
            slave_return_list.append((host, port))
        return slave_return_list

def main():
    try:
        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            port_num = int(sys.argv[1])
            server_req = FileServer(port_num)
        else:
            server_req = FileServer()
        server_req.listen()
    except socket.error, msg:
        print "Unable to create socket connection: " + str(msg)
        con = None

if __name__ == "__main__": main()
