
import socket
import sys
import os
import re
import threading
import time
import Queue
import base64

# Regex used for the testing of the client proxy
Upload_Command = "upload [a-zA-Z0-9_]*."
Download_Command = "download [a-zA-Z0-9_]*."
Directory_Command = "dir [a-zA-Z0-9_/.]*"
Lock_Command = "lock [a-zA-Z0-9_/.]* [0-9]*"




class TCPClient:
    PortNum = 8000
    HOST = "0.0.0.0"
    Directory_Port = 7333
    FileServer_Port = 7001
    Directory_Host = HOST
    Lock_port_num = 7334
    Lock_host_num = HOST
    Lock_header = "LOCK_FILE: %s\nTime: %d\n\n"
    Lock_rep = "LOCK_RESPONSE: \nFILENAME: .*\nTIME: .*\n\n"
    Unlock_info_header = "UNLOCK_FILE: %s\n\n"
    Upload_info_header = "UPLOAD: %s\nDATA: %s\n\n"
    Download_info_header = "DOWNLOAD: %s\n\n"
    Directory_info_header = "GET_SERVER: \nFILENAME: %s\n\n"
    Server_rep = "PRIMARY_SERVER: .*\nPORT: .*\nFILENAME: .*"
    Fail_rep = "ERROR: .*\nMESSAGE: .*\n\n"
    Req_info = "%s"
    Max_length = 4096
    Client_root = os.getcwd()
    File_location = "ClientFiles"
    File_path = os.path.join(Client_root, File_location)

    def __init__(self, port_use=None):
        if not port_use:
            self.port_use = self.PortNum
        else:
            self.port_use = port_use
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.open_files = {}
        self.threadQueue = Queue.Queue()

    def __send_request(self, data, server, port):
        """Function that sends requests to remote server"""
        return_info_data = ""
        sock_req = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_req.connect((server, port))
        sock_req.sendall(self.Req_info % data)
        # Loop until all data received
        while "\n\n" not in return_info_data:
            data = sock_req.recv(self.Max_length)
            if len(data) == 0:
                break
            return_info_data += data
        # Close and dereference the socket
        sock_req.close()
        sock_req = None
        return return_info_data

    def __raw_request(self, string):
        """Send a raw request to remote server"""
        return_data_info = ""
        # Do nothing if the string is empty or socket doesn't exist
        if len(string) > 0:
            # Create socket if it doesn't exist
            return_data_info = self.__send_request(string + "\n\n")
        return return_data_info

    def __upload_file(self, server, filename):
        """Send a request to the server to upload a file"""
        path_u = os.path.join(self.File_path, filename)
        file_handle = open(path_u, "rb")
        # Base64 encode the file so it can be sent in a message
        data_upload = file_handle.read()
        data_upload = base64.b64encode(data_upload)
        request = self.Upload_info_header % (filename, data_upload)
        return self.__send_request(request, server, self.FileServer_Port)

    def __download_file(self, server, port, filename):
        """Send a request to the server to download a file"""
        path_info = os.path.join(self.File_path, filename)
        # Download message containing file data and then base64 decode the data
        request = self.Download_info_header % (filename)
        request_data = self.__send_request(request, server, port).splitlines()[0]
        data = request_data.split()[0]
        data = base64.b64decode(data)
        file_handle = open(path_info, "wb+")
        file_handle.write(data)
        return True

    def __lock_file(self, filename, lock_time):
        """Send a request to the server to locks a file"""
        request = self.Lock_header % (filename, lock_time)
        request_data = self.__send_request(request, self.Lock_host_num, self.Lock_port_num)
        if re.match(self.Fail_rep, request_data):
            # If failed to lock the file, wait a time and try again
            request_data = request_data.splitlines()
            wait_time_total = float(request_data[1].split()[1])
            time.sleep(wait_time_total)
            self.__lock_file(filename, lock_time)
        return True


    def __unlock_file(self, filename):
        """Send a request to the server to unlock a file"""
        request = self.Unlock_info_header % filename
        return self.__send_request(request, self.Lock_host_num, self.Lock_port_num)

    def open(self, filename):
        file_download = False
        if filename not in self.open_files.keys():
            request_info = self.__get_directory(filename)
            if re.match(self.Server_rep, request_info):
                params_open = request_info.splitlines()
                server = params_open[0].split()[1]
                port = int(params_open[1].split()[1])
                open_file = params_open[2].split()[1]
                self.__lock_file(filename, 10)
                file_download = self.__download_file(server, port, open_file)
                if file_download:
                    self.open_files[filename] = open_file
        return file_download

    def close(self, filename):
        file_upload = False
        if filename in self.open_files.keys():
            request = self.__get_directory(filename)
            if re.match(self.Server_rep, request):
                self.__unlock_file(filename)
                params_cloase = request.splitlines()
                server = params_cloase[0].split()[1]
                open_file = params_cloase[2].split()[1]
                file_upload = self.__upload_file(server, open_file)
                if file_upload:
                    path_u = os.path.join(self.Client_root, self.File_location)
                    path_u = os.path.join(path_u, self.open_files[filename])
                    if os.path.exists(path_u):
                        os.remove(path_u)
                    del self.open_files[filename]
        return file_upload

    def read(self, filename):
        """Function that reads from an open file"""
        if filename in self.open_files.keys():
            local_name_read = self.open_files[filename]
            path = os.path.join(self.File_path, local_name_read)
            file_handle_info = open(path, "rb")
            data = file_handle_info.read()
            return data
        return None

    def write(self, filename, data):
        """Function that writes to an open file"""
        success_write = False
        if filename in self.open_files.keys():
            local_name_write = self.open_files[filename]
            path_info = os.path.join(self.File_path, local_name_write)
            file_handle = open(path_info, "wb+")
            file_handle.write(data)
            success_write = True
        return success_write

    def __get_directory(self, filename):
        """Send a request to the server to find the location of a directory"""
        request = self.Directory_info_header % filename

        return self.__send_request(request, self.Directory_Host, self.Directory_Port)



class ThreadHandler(threading.Thread):
    def __init__(self, thread_queue, buffer_length, server):
        threading.Thread.__init__(self)
        self.queue = thread_queue
        self.bufferLength = buffer_length
        self.server = server

    def run(self):
        # Thread loops and waits for connections to be added to the queue
        while True:
            request = self.queue.get()
            self.handler(request)
            self.queue.task_done()

    def handler(self, (con, addr)):
        message = ""
        # Loop and receive data
        while True:
            data = con.recv(1024)
            message += data
            if len(data) < self.bufferLength:
                break

        # If valid http request with message body
        if len(message) > 0:
            print "fffa"
            # Handle diff messages
        return

def main():

    try:
        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            port_m_num = int(sys.argv[1])
            con_client_ = TCPClient(port_m_num)
        else:
            con_client_ = TCPClient()
    except socket.error, msg:
        print "Unable to create socket connection: " + str(msg)
        con_client_ = None
    while con_client_:
        user_input = raw_input("Please input command(upload or download) + filename to upload the file.\nIf you want to exit, please input 'leave'\n :")
        if user_input.lower() == "leave":
            con_client_ = None
        elif re.match(Upload_Command, user_input.lower()):
            request = user_input.lower()
            path_info = os.path.join(TCPClient.File_path, user_input.lower().split()[1])
            file_handle = open(path_info, "rb")
            data = file_handle.read()
            file_name = request.split()[1]
            con_client_.open(file_name)
            con_client_.write(file_name, data)
            con_client_.close(file_name)
        elif re.match(Download_Command, user_input.lower()):
            request = user_input.lower()
            file_name = request.split()[1]
            con_client_.open(file_name)
            con_client_.close(file_name)
        elif re.match(Directory_Command, user_input.lower()):
            print('Show the directory of the file: \n')
            request = user_input.lower()
            file_name = request.split()[1]
            con_client_.open(file_name)
            con_client_.close(file_name)
        elif re.match(Lock_Command, user_input.lower()):
            print('Locking the file....... \n')
            request = user_input.lower()
            file_name = request.split()[1]
            con_client_.open(file_name)
            con_client_.close(file_name)
        else:
            data = con_client_._TCPClient__raw_request(user_input)
            print data

if __name__ == "__main__": main()

