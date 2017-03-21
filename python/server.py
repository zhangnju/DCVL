#!/usr/bin/env python
import os
import sys 
import socket
import thread

#get local IP,according to prefix value for mutiple cards
def GetLocalIPByPrefix(prefix):
    addr = ''
    for ip in socket.gethostbyname_ex(socket.gethostname())[2]:
        if ip.startswith(prefix):
            addr = ip
    return addr

#get local IP for single card
def GetLocalIP():
    try:
        csock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        csock.connect(('8.8.8.8', 80))
        (addr, port) = csock.getsockname()
        csock.close()
        return addr
    except socket.error:
        return "127.0.0.1"

#here define a simple Toplogy to test
def ParseToplogy(toplogy):
    #info=[name,spout_count,bolt0_count,bolt1_count]
    info=pickle.loads(toplogy)
    return info

#Function for handling connections. This will be used to create threads
def clientthread(conn):
    #Sending message to connected client
    #conn.send('Welcome to the server. Type something and hit enter\n') 
     
    #infinite loop so that function do not terminate and thread do not end.
    while True:
        #Receiving from client
        data = conn.recv(BUFSIZ)
        data_type=pickle.loads(data)
        if data_type is 'toplogy':
           data = conn.recv(BUFSIZ) 
           ParseToplogy(data)
        conn.sendall('OK')
    conn.close()

#parse the config file, and then start master/worker
conf={}
with open('cluster.conf', 'rt') as f:
    for line in f:
        setting=line.split(":")
        conf.setdefault(setting[0],[]).append(setting[1].strip())
print conf['master_host']#just for debugging
print conf['worker_host']#just for debugging
tmp=" ".join(str(x) for x in conf['master_count'])
if tmp.isdigit():
   master_count=int(tmp)
else:
   print 'wrong master count setting'

tmp=" ".join(str(x) for x in conf['worker_count'])
if tmp.isdigit():
   worker_count=int(tmp)
else:
   print 'wrong worker count setting'
   
localIP=GetLocalIP()

for master in conf['master_host']:
   if(str(master)is localIP):
       IsMaster=True
   else:
       IsMaster=False

for worker in conf['worker_host']:
   if(str(worker)is localIP):
       IsWorker=True
   else:
       IsWorker=False

#if IsMaster:
#    os.system('master.exe')

#if IsWorker:
#    os.system('worker.exe')

if IsMaster:
    HOST = ' localhost '
    PORT = 21567
    BUFSIZ = 1024
    ADDR = (HOST, PORT)
    tcpSerSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      tcpSerSock.bind(ADDR)
    except socket.error as msg:
      print 'Bind failed.Error Code: '+str(msg[0])+' Message'+str(msg[1])
      sys.exit()
    print 'Socket bind done'
    tcpSerSock.listen(5)
    print 'Server now listening'

    while True:
       #wait to accept a connection - blocking call
       conn, addr = tcpSerSock.accept()
       print 'Connected with ' + addr[0] + ':' + str(addr[1])
       #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
       start_new_thread(clientthread ,(conn,))
       
    tcpSerSock.close()
