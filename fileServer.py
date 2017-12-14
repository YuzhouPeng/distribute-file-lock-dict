
import socket
import os
import re
import sys
import base64

from tcpServer import TCPServer


class FileServer(TCPServer):
    Upload_info = "UPLOAD: [a-zA-Z0-9_.]*\nDATA: .*\n\n"
    Update_info = "UPDATE: [a-zA-Z0-9_.]*\nDATA: .*\n\n"
    Update_header = "UPDATE: %s\nDATA: %s\n\n"
    Download_info = "DOWNLOAD: [a-zA-Z0-9_.]*\n\n"
    Download_rep = "DATA: %s\n\n"
    Get_slave_header = "GET_SLAVES: %s\nPORT: %s\n\n"
    Upload_rep = "OK: 0\n\n"
    Server_root_info = os.getcwd()
    Server_file_name = "DirectoryServerFiles"
    File_location = os.path.join(Server_root_info, Server_file_name)
    Host_dir = "0.0.0.0"
    Port_dir = 7333

    def __init__(self, port_use=None):
        TCPServer.__init__(self, port_use, self.handler)
        self.File_location = os.path.join(self.File_location, str(self.Port_num))

    def handler(self, message, con, addr):
        if re.match(self.Upload_info, message):
            self.upload(con, addr, message)
        elif re.match(self.Download_info, message):
            self.download(con, addr, message)
        elif re.match(self.Update_info, message):
            self.update(con, addr, message)
        else:
            return False

        return True

    def upload(self, con, addr, text):
        # Handler for file upload requests
        filename, data = self.execute_write(text)
        return_string_rep = self.Upload_rep
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
        return_string_info = self.Download_rep % (base64.b64encode(data))
        con.sendall(return_string_info)
        return

    def update(self, con, addr, text):
        # Handler for file update requests
        self.execute_write(text)
        return_string_info = self.Upload_rep
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
        update_s = self.Update_header % (filename, base64.b64encode(data))
        for (host, port) in slaves:
            self.send_request(update_s, host, int(port))
        return

    def get_slaves(self):
        # Function to get the list of slave file servers
        slave_return_list = []
        request_data = self.Get_slave_header % (self.Host_num, self.Port_num,)
        lines_req = self.send_request(request_data, self.Host_dir, self.Port_dir).splitlines()
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
