#!/usr/bin/python


import sys
import socket
import asyncore, asynchat

from threading import Thread

class ScriptOutputThread(Thread):
    def init(this):
        class log_server(asyncore.dispatcher):
            def __init__(self):
                asyncore.dispatcher.__init__(self)
                self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
                self.bind(this.serverAddress)
                if this.serverAddress[1] == 0:
                    this.serverAddress = (this.serverAddress[0], self.socket.getsockname()[1])
                self.listen(10)
            def handle_accept(self):
                log_handler(*self.accept())
            def handle_close(self):
                self.close()

        class log_handler(asynchat.async_chat):
            def __init__(self, sock, address):
                asynchat.async_chat.__init__(self, sock = sock)
                self.set_terminator("\n")
                self.address = "%s:%d" % address
                self.ibuffer = []
            def collect_incoming_data(self, data):
                self.ibuffer.append(data)
            def found_terminator(self):
                this.fileObject.write("%s> %s\n" % (self.address, ''.join(self.ibuffer).rstrip()))
                self.ibuffer = []

        this.serv = log_server()

    def run(self):
        try:
            while not self.finished:
                asyncore.loop(timeout = 1, count = 1)
        finally:
            self.serv.close()
            del self.serv
            asyncore.close_all()

def outputService():
    """Start a standalone output service

    This service can be used in an other Python or R instance, for
    Python instances the connection parameter externalClient need to
    be specified.
    """
    try: host = socket.gethostbyname(socket.gethostname())
    except: host = '0.0.0.0'

    from optparse import OptionParser
    parser = OptionParser(description =
                          """This script binds to IP and port and outputs everything it gets from
                          the connections to stdout with all lines prefixed with client address.""")
    parser.add_option("-s", "--server", dest="server", metavar="SERVER", type="string",
                      default=host,
                      help="hostname or IP address to bind to (default: %default)")
    parser.add_option("-p", "--port", dest="port", metavar="PORT", type="int", default=3000,
                      help="port number to bind to (default: %default)")
    #(options, args) = parser.parse_args()
    options = parser.parse_args()[0]
    address = options.server, options.port
    sys.stdout.flush()
    server = ScriptOutputThread()
    server.serverAddress = address
    server.fileObject = sys.stdout
    server.finished = False
    server.init()
    print ">>> bind the output server to %s:%d" % server.serverAddress
    sys.stdout.flush()
    try: server.run()
    except KeyboardInterrupt:
        sys.stdout.flush()
    sys.exit(0)



outputService()
