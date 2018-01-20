# -*- coding: utf-8 -*-
"""
Created on Sun Jan 14 00:59:41 2018

A really convoluted :P program to download files from a sftp server using multiple threads

@author: nasinha
"""

import paramiko
import time
import threading
import os
import sys,traceback
import getopt
from paramiko import AuthenticationException
from socket import gaierror

hostname = ""
user     = ""
pswd     = ""
target   = "" 
outputdir= ""

def parse_cmd(argv):
   global pswd
   global user
   global hostname
   global target
   
   try:
      opts, args = getopt.getopt(argv,"u:p:h:f:",["user=","pswd=","host=","fname="])
   except getopt.GetoptError:
      print('Valid Arguments :  -u <username> -p <password> -h <hostname> -f <"File/Name">')
      sys.exit(2)
   for opt, arg in opts:
       if opt in ("-u", "--user"):
           user = arg
       elif opt in ("-p", "--pswd"):
           pswd = arg
       elif opt in ("-h","--host"):
           hostname = arg
       elif opt in ("-f","--fname"):
           target = arg
   while(len(user)==0):
       user= input("Username :")
   while(len(pswd)==0):
       pswd = input("Password :")
   while(len(hostname)==0):
       hostname= input("Hostname :")
   while(len(target)==0):
       target= input("File path :")

def getInput(message):
   inps = ""
   while(len(inps)==0):
       inps = input(message)
   return inps
       
def split(size,blocks):
    sz = int(size/blocks)
    ret = []
    init = 0
    for x in range(0,blocks-1):
        ret.append((init,init+sz,x))
        init = init+sz
    ret.append((init,size,blocks-1))
    return ret

def printTotals(transferred, toBeTransferred):
    print("Transferred: {0}\tOut of: {1}".format(transferred, toBeTransferred))
    
progress_bar = []

def monitor_progress(thread_count,fileSize,startTime):
    while(True):
        
        s = (sum(progress_bar)/thread_count*1.0)*100
        left = ((100-s)*fileSize)/102400.0
        speed= (s*fileSize/100)/((time.time()+1)-startTime)
        #print(s,left,speed)
        if(speed !=0 ):
            eta  = left*1024/speed
            print("{0:.2f}% Completed | {1:.2f} KB Left  | ETA {2:.2f} Seconds                                                    ".format(s,left,eta),end="\r")
        else:
            print("{0:.2f}% Completed | {1:.2f} KB Left                                                    ".format(s,left),end="\r")
        if(s>=100):
            print("                                                                                                                ")
            break
        time.sleep(1)        
    
failed_list = []

def download_part(hostname,port,user,pswd,filepath,start, end,thread_id):
    
    #print("FILE == > ",start,end,thread_id,end-start)

    try:
        paramiko.util.log_to_file('sftp downloader.log')
        transport = paramiko.Transport((hostname, 22))
        transport.connect(username = user, password = pswd)
        sftp = paramiko.SFTPClient.from_transport(transport)
        #sftp.get_channel().settimeout(5000)
        #print(sftp.get_channel().gettimeout())
        handle1 = sftp.open(filepath)
        
        s1 = handle1.readv([(start,end-start)])
        
        writeto_ = open("thread_id_"+str(thread_id),"wb")
        for x in s1:
            writeto_.write(x)
        writeto_.close()
        sftp.close()
        
        #print("Thread "+str(thread_id)+" finished")
        failed_list.remove((start,end,thread_id))
        progress_bar[thread_id] = 1
    except Exception as e:
        pass
        #print("Thread ",thread_id," failed.")
        #failed_list.append((start,end))

def concatenateFile(filenames,target):
    #filenames = list(set(filenames))
    target = target.split("/")
    target = target[len(target)-1]
    fullPath = ""
    with open(target, 'wb') as outfile:
        try:
            fullPath = outfile.name
            for fname in filenames:
                with open(fname,"rb") as infile:
                    outfile.write(infile.read())
                os.remove(fname)
        except:
            traceback.print_exc(file=sys.stdout)
    return fullPath

parse_cmd(sys.argv[1:])

def AuthenticateAndSendSFTPObj():
    global hostname
    global user
    global pswd

    paramiko.util.log_to_file('sftp downloader.log')
    for i in range(0,5):
        try:
            transport = paramiko.Transport((hostname, 22))
            transport.connect(username = user, password = pswd)
            return paramiko.SFTPClient.from_transport(transport)
        except AuthenticationException as e:
            print("Wrong Username/Password Combination ",e)
            user = getInput("Username :")
            pswd = getInput("Password :")
        except gaierror as e:
            print("Network level Exception Verify if the hostname provided is correct ",e)
            hostname = getInput("Hostname :")
        except Exception as e:
            print("Unhandled Exception ",e,e.with_traceback)
            sys.exit()
            
        
sftp = AuthenticateAndSendSFTPObj()

file_stat  = ""

try:
    while(True):
        try:
            handle = sftp.open(target,"rb")
        except FileNotFoundError as e:
            print("File not found ",e)
            target = getInput("File Path :")
            continue
        except Exception as e:
            print("Unhandled Exception ",e,e.with_traceback)
            sys.exit()
            
            
        file_stat = handle.stat()
        print(file_stat)
        
        file_stat = str(file_stat)
        if(file_stat[0]=='d'):
            print("The file name provided is a directory. Provide a valid file name.")
            target = input("File Name :")
            handle.close()
            
        else:
            sftp.close()
            break
        
    
    start_time = time.time()
    fileSZ = str(file_stat).split()
    fileSZ = int(fileSZ[4])
    
    print("File size : ",fileSZ/(1024*1024.0)," Mb")
    print("File Name : ",target)
    
    split_count = 30
    pointer_split = split(fileSZ,split_count)
    progress_bar  = [0 for x in range(0,split_count)]
    threads = []
    filenames = []
    
    
    failed_list = pointer_split
    
    monitoringThread = threading.Thread(target=monitor_progress,name="Monitoring",args=(split_count,fileSZ,time.time()))
    monitoringThread.start()
    
    initial_Round = True
    
    while(len(pointer_split)>0):
        for t in range(0,len(pointer_split)):
            
            connect_suceeded = 0
            thr = threading.Thread(target=download_part,name="thr_"+str(t),args=(hostname,22,user,pswd,target
                                   ,pointer_split[t][0],pointer_split[t][1],pointer_split[t][2]))
            
            if(initial_Round):filenames.append("thread_id_"+str(t))
            thr.start()
            threads.append(thr)
            #print("Thread "+str(t)+" started")
        for t in threads:
            t.join()
        initial_Round = False
except:
    pass
print("===============================================")
print("Took {0:.2f} Seconds to fetch.".format(time.time()-start_time))
path = concatenateFile(filenames,target)
print("Took {0:.2f} Seconds to Write.".format(time.time()-start_time))
print("File Saved at : ",os.path.abspath(path))
print("===============================================")
sys.exit()
