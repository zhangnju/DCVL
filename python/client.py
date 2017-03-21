#!/usr/bin/env python

import socket
try:
   import cPickle as pickle
except:
   import pickle

#here define a simple Toplogy to test
def GenToplogy(name,spout_count,bolt0_count,bolt1_count):
    info=[name,spout_count,bolt0_count,bolt1_count]
    toplogy=pickle.dumps(info)
    return toplogy

def SubmitToplogy(host,port,toplogy):
    data_type=pickle.dumps('toplogy')
    BUFSIZ = 1024
    ADDR = (host, port)
    tcpCliSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpCliSock.connect(ADDR)
    tcpCliSock.send(data_type)
    tcpCliSock.send(toplogy)
    status = tcpCliSock.recv(BUFSIZ)
    tcpCliSock.close()
