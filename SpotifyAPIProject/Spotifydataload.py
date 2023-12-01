import os
import shutil
import snowflake.connector as con
import json
from datetime import datetime
import time
import hashlib
import bcrypt
import credentials
from threading import Thread
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa,padding
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization,hashes

albumdirectory=credentials.albumdirectory
artistdirectory=credentials.artistdirectory
songsdirectory=credentials.songsdirectory
albumprocessedpath=credentials.albumprocessedpath
artistprocessedpath=credentials.artistprocessedpath
songprocessedpath=credentials.songprocessedpath
stage_name=credentials.stage_name
#stageload=credentials.stageload
#copycommand=credentials.copycommand
albumtable=credentials.albumtable
artisttable=credentials.artisttable
songtable=credentials.songtable

def generate_asymmetric_keypair():
    private_key=rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
    )
    public_key=private_key.public_key()
    return private_key,public_key

def encrypt_message_rsa(message,public_key):
    encrypted_message=public_key.encrypt(
            message.encode(),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
    )
    return encrypted_message

def decrypt_message_rsa(encrypted_message,private_key):
    decrypted_message=private_key.decrypt(
            encrypted_message,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
    ).decode()

    return decrypted_message
try:
    def album_load(albumdirectory,cur,albumprocessedpath):
        os.chdir(albumdirectory)
        #print(os.getcwd())
        files=os.listdir()
        for file in files:
            # print(file)
            #time.sleep(1)
            if file=='':
                print("All data is loaded into Snowflake")
            else:
                try:
                    print("put file://"+albumdirectory+file+ " @"+stage_name+";")
                    cur.execute("put file://"+albumdirectory+file+ " @"+stage_name+";")
                    #cur.execute("copy into @"+stage_name+"/"+file+"*"+" into "+albumtable+";")
                    print("copy into "+albumtable+" from @"+stage_name+"/"+file+".gz;")
                    cur.execute("copy into "+albumtable+" from @"+stage_name+"/"+file+".gz;")
                    shutil.move(file,albumprocessedpath)
                    print("Album load is completed")
                    #time.sleep(5)
                except Exception as e:
                    print("Error ocurred while loading... {0}".format(e))
                    cur.execute("TRUNCATE TABLE "+albumtable+";")
                    shutil.move(albumprocessedpath+"/"+file,albumdirectory)

    def artist_load(artistdirectory,cur,artistprocessedpath):
        os.chdir(artistdirectory)
        #print(os.getcwd())
        files=os.listdir()
        for file in files:
            #print(file)
            #time.sleep(1)
            if file=='':
                print("All data is loaded into Snowflake")
            else:
                try:
                    print("put file://"+artistdirectory+file+ " @"+stage_name+";")
                    cur.execute("put file://"+artistdirectory+file+ " @"+stage_name+";")
                    #cur.execute("copy into @"+stage_name+"/"+file+"*"+" into "+artisttable+";")
                    print("copy into "+artisttable+" from @"+stage_name+"/"+file+".gz;")
                    cur.execute("copy into "+artisttable+" from @"+stage_name+"/"+file+".gz;")
                    shutil.move(file,artistprocessedpath)
                    print("Artist load is completed")
                    #time.sleep(5)
                except Exception as e:
                    print("Error occured while loading... {0}".format(e))
                    cur.execute("TRUNCATE TABLE "+artisttable+";")
                    shutil.move(artistprocessedpath+"/"+file,artistdirectory)


    def song_load(songdirectory,cur,songprocessedpath):
        os.chdir(songdirectory)
        #print(os.getcwd())
        files=os.listdir()
        for file in files:
            #print(file)
            #time.sleep(1)
            if file=='':
                print("All data is loaded into Snowflake")
            else:
                try:
                    print("put file://"+songdirectory+file+ " @"+stage_name+";")
                    cur.execute("put file://"+songdirectory+file+ " @"+stage_name+";")
                    #cur.execute("copy into @"+stage_name+"/"+file+"*"+" into "+songtable+";")
                    print("copy into "+songtable+" from @"+stage_name+"/"+file+".gz;")
                    cur.execute("copy into "+songtable+" from @"+stage_name+"/"+file+".gz;")
                    shutil.move(file,songprocessedpath)
                    print("Song load is completed")
                except Exception as e:
                    print("Error occured while loading... {0}".format(e))
                    cur.execute("TRUNCATE TABLE "+songtable+";")
                    shutil.move(songprocessedpath+"/"+file,songdirectory)
except:
    cur.execute("REMOVE @"+stage_name+";")



private_key,public_key=generate_asymmetric_keypair()
message_rsa=credentials.sfpassword

encrypted_message_rsa=encrypt_message_rsa(message_rsa,public_key)
#print(encrypted_message_rsa)

decrypted_message_rsa=decrypt_message_rsa(encrypted_message_rsa,private_key)
#print(decrypted_message_rsa)
try:
    connection=con.connect(
        user=credentials.username,
        database=credentials.database,
        schema=credentials.schema,
        warehouse=credentials.warehouse,
        account=credentials.account,
        password=decrypted_message_rsa
    )
    #print(username)
    cur=connection.cursor()
    album_load(albumdirectory,cur,albumprocessedpath)
    artist_load(artistdirectory,cur,artistprocessedpath)
    song_load(songsdirectory,cur,songprocessedpath)
    #album_Thread=Thread(target=album_load,args=(albumdirectory,cur,albumprocessedpath))
    #artist_Thread=Thread(target=artist_load,args=(artistdirectory,cur,artistprocessedpath))
    #song_Thread=Thread(target=song_load,args=(songsdirectory,cur,songprocessedpath))
    #album_Thread.start()
    #time.sleep(2)
    #artist_Thread.start()
    #time.sleep(2)
    #song_Thread.start()

    #album_Thread.join()
    #artist_Thread.join()
    #song_Thread.join()
    cur.close()
    connection.close()
except Exception as e:
    print("Error in connecting to Snowflake: {0}".format(e))