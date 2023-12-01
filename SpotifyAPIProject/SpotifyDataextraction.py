import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import json
import credentials
from datetime import datetime
from random import  randint
import os
import shutil
import re
#upload the data to blob

def uploaddatatoblob(data_file,connectionString,storageaccountname,storageaccountkey,containername,filename):
        data=json.dumps(data_file)
        #filename = re.sub(r'[^\w\s-]','',"spotify_rawdata_"+str(datetime.now()).strip().replace('',''))+''+'.json'
        blob_service_client=BlobServiceClient.from_connection_string(connectionString)
        blob_client=blob_service_client.get_blob_client(container=container_name,blob=filename)
        blob_client.upload_blob(data)
        #shutil.move(filename,credentials.rawdirectory)
        print("Data uploaded to blob successfully")
                        #credentials

client_credentials = SpotifyClientCredentials(client_id=credentials.clientid,client_secret=credentials.clientsecret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials)
playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"
playlist_code = playlist_link.split("/")[-1]
cwd=os.getcwd()
#extract the tracks
sp_data = sp.playlist_tracks(playlist_code)
#azure credentials
#data=json.dumps(sp_data)
filename = (re.sub(r'[^\w\s-]','',"spotify_rawdata_"+str(datetime.now()).strip().replace('',''))+''+'.json').replace(' ','_')
os.mknod(cwd+'/'+f"{filename}")
with open(filename,'w') as b:
    json.dump(sp_data,b,indent=4)
shutil.move(filename,credentials.rawdirectory)
token_credential=DefaultAzureCredential()
storage_account_key=credentials.storageaccountkey
storage_account_name=credentials.storageaccountname
connection_string=credentials.connectionstring
container_name=credentials.containername
uploaddatatoblob(sp_data,connection_string,storage_account_name,storage_account_key,container_name,filename)
