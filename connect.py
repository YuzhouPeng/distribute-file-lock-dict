
import socket
import threading
import Queue
import os
import re
import sys


class Connect(object):
    Port_num = 8000
    Host_num = '0.0.0.0'
    def __init__(self, port_use=None, handler=None):
        if not port_use:
            port_use = 8000
        else:
            self.Port_num = port_use
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('0.0.0.0', port_use))
        self.handler = handler if handler else self.default_handler
        # Create a queue of tasks with ma
        self.threadQueue = Queue.Queue(maxsize=8)

        # Create thread pool
        for i in range(8):
            thread = ThreadHandler(self.threadQueue, 4096, self)
            thread.setDaemon(True)
            thread.start()

    def default_handler(self, message, con, addr):
        return False

    def listen(self):
        self.sock.listen(5)

        # Listen for connections and delegate to threads
        while True:
            con_listen, addr_listen = self.sock.accept()
            # If queue full close connection, otherwise send to thread
            if not self.threadQueue.full():
                self.threadQueue.put((con_listen, addr_listen))
            else:
                print "Queue full closing connection from %s:%s" % (addr_listen[0], addr_listen[1])
                con_listen.close()

    def kill_serv(self, con):
        # Kill server
        os._exit(1)
        return

    def helo(self, con, addr, text):
        # Reply to helo request
        reply_h = text.rstrip()  # Remove newline
        reply_h = reply_h.split()[1]
        return_string_info = "HELO %s\nIP:%s\nPort:%s\nStudentID:17316109" % (reply_h, addr[0], addr[1])
        con.sendall(return_string_info)
        return

    def default(self, con, addr, text):
        return_string_info = "input invalid command\n\n"
        con.sendall(return_string_info)
        # Default handler for everything else
        print "Default"
        return

    Max_length = 2048
    def send_request(self, data, server, port):
        return_data_req = ""
        sock_info = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_info.connect((server, port))
        sock_info.sendall(data)
        # Loop until all data received
        while "\n\n" not in return_data_req:
            data = sock_info.recv(self.Max_length)
            if len(data) == 0:
                break
            return_data_req += data

        # Close and dereference the socket
        sock_info.close()
        sock_info = None
        return return_data_req


class ThreadHandler(threading.Thread):
    def __init__(self, thread_queue, buffer_length, server):
        threading.Thread.__init__(self)
        self.queue = thread_queue
        self.buffer_length = buffer_length
        self.server = server
        self.messageHandler = server.handler

    def run(self):
        # Thread loops and waits for connections to be added to the queue
        while True:
            request_info = self.queue.get()
            self.handler(request_info)
            self.queue.task_done()

    def handler(self, (con, addr)):
        message = ""
        # Loop and receive data
        while "\n\n" not in message:
            data = con.recv(self.buffer_length)
            message += data
            if len(data) < self.buffer_length:
                break
        # If valid http request with message body
        if len(message) > 0:
            if message == "KILL_SERVICE\n":
                print "Killing service"
                self.server.kill_serv(con)
            elif re.match("HELO .*", message):
                self.server.helo(con, addr, message)
            elif self.messageHandler(message, con, addr):
                None
            else:
                print message
                self.server.default(con, addr, message)
        return

def main():
    try:
        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            port_num = int(sys.argv[1])
            server_info = Connect(port_num)
        else:
            server_info = Connect()
        server_info.listen()
    except socket.error, msg:
        print "Unable to create socket connection: " + str(msg)

if __name__ == "__main__": main()
