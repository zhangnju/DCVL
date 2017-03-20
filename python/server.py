#!/usr/bin/env python

from socket import *
from sys import *
from thread import *

HOST = ''
PORT = 21567
BUFSIZ = 1024
ADDR = (HOST, PORT)

tcpSerSock = socket(AF_INET, SOCK_STREAM)
try:
    tcpSerSock.bind(ADDR)
except tcpSerSock.error as msg:
    print 'Bind failed.Error Code: '+str(msg[0])+' Message'+str(msg[1])
    sys.exit()
print 'Socket bind done'
tcpSerSock.listen(5)
print 'Server now listening'

#Function for handling connections. This will be used to create threads
def clientthread(conn):
    #Sending message to connected client
    conn.send('Welcome to the server. Type something and hit enter\n') 
     
    #infinite loop so that function do not terminate and thread do not end.
    while True:
        #Receiving from client
        data = conn.recv(BUFSIZ)
        reply = 'OK...' + data
        if not data: 
            break
        conn.sendall(reply)
    conn.close()

while True:
    #wait to accept a connection - blocking call
    conn, addr = tcpSerSock.accept()
    print 'Connected with ' + addr[0] + ':' + str(addr[1])
     
    #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
    start_new_thread(clientthread ,(conn,))
 
s.close()
