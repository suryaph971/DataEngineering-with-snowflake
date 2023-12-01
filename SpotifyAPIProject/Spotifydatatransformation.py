import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import json
import pandas as pd
import credentials
import shutil
from datetime import datetime
import re
import os
import glob

sourcepath=credentials.sourcepath
storage_account_key=credentials.storageaccountkey
storage_account_name=credentials.storageaccountname
connection_string=credentials.connectionstring
container_name=credentials.containername
targetContainerName=credentials.processedcontainername
rawDirectory=credentials.rawdirectory
albumContainerName=credentials.albumcontainername
artistContainerName=credentials.artistcontainername
songContainerName=credentials.songscontainername
#processedContainerName=credentials.processedcontainername
processedDirectory=credentials.processeddirectory
albumDirectory=credentials.albumdirectory
artistDirectory=credentials.artistdirectory
songDirectory=credentials.songsdirectory

def uploaddatatoblob(file,sourcepath,storage_account_key,storage_account_name,connection_string,container_name,targetContainerName,rawDirectory,targetfileContainerName,processedDirectory,targetDirectory,datafile,name):
    filename=name+'_'+file.split('/')[-1].split('_')[2]+'_'+file.split('/')[-1].split('_')[3]
    blob_service_client=BlobServiceClient.from_connection_string(connection_string)
    blob_client=blob_service_client.get_blob_client(container=targetfileContainerName,blob=filename)
    #blob_client.upload_blob(datafile)
    #os.mknod(processedDirectory+f"{filename}")
    destfilename=targetDirectory+filename
    datafile.to_csv(destfilename+'.csv')
    #print("Data uploaded successfully")
    os.chdir(targetDirectory)
    blob_client.upload_blob(destfilename+'.csv')
    return 1;

def transformation(file,sourcepath,storage_account_key,storage_account_name,connection_string,container_name,targetContainerName,rawDirectory,artistContainerName,albumContainerName,songContainerName,processedDirectory,albumDirectory,artistDirectory,songDirectory):
    #print(file)
    with open(file,'r') as jsonfile:
        jsonfile=json.load(jsonfile)
        album_list=[]
        for row in jsonfile['items']:
            album_id=row['track']['album']['id']
            album_name=row['track']['album']['name']
            album_release_date=row['track']['album']['release_date']
            album_total_tracks=row['track']['album']['total_tracks']
            album_url=row['track']['album']['external_urls']['spotify']
        #print(album_id)
            albums = {'album_id':album_id,'album_name':album_name,'album_release_date':album_release_date,'album_total_tracks':album_total_tracks,'album_url':album_url}

        #print(albums)
            album_list.append(albums)

        #print(album_list)

        artist_list=[]
        for row in jsonfile['items']:
            for key,value in row.items():
                if key=='track':
                    for artist in value['artists']:
                        artist_dict = {'artist_id':artist['id'],'artist_name':artist['name'],'external_url':artist['href']}
                        artist_list.append(artist_dict)
        #print(artist_list)

        songs_list=[]
        for row in jsonfile['items']:
            song_id = row['track']['id']
            song_name = row['track']['name']
            song_duration = row['track']['duration_ms']
            song_url = row['track']['external_urls']['spotify']
            song_popularity=row['track']['popularity']
            song_added=row['added_at']
            album_id=row['track']['album']['id']
            artist_id=row['track']['album']['artists'][0]['id']
            song_element={'song_id':song_id,'song_name':song_name,'song_duration':song_duration,'song_url':song_url,'song_popularity':song_popularity,'song_added':song_added,'album_id':album_id,'artist_id':artist_id}

            songs_list.append(song_element)
        #print(songs_list)
        album_df=pd.DataFrame.from_dict(album_list)
        artist_df=pd.DataFrame.from_dict(artist_list)
        song_df=pd.DataFrame.from_dict(songs_list)

        album_df.drop_duplicates(subset=['album_id'])
        artist_df.drop_duplicates(subset=['artist_id'])
        song_df.drop_duplicates(subset=['song_id'])

        album_df['album_release_date']=pd.to_datetime(album_df['album_release_date'])
        song_df['song_added']=pd.to_datetime(song_df['song_added'])

        if(uploaddatatoblob(file,sourcepath,storage_account_key,storage_account_name,connection_string,container_name,targetContainerName,rawDirectory,albumContainerName,processedDirectory,albumDirectory,album_df,'album') and uploaddatatoblob(file,sourcepath,storage_account_key,storage_account_name,connection_string,container_name,targetContainerName,rawDirectory,artistContainerName,processedDirectory,artistDirectory,artist_df,'artist') and uploaddatatoblob(file,sourcepath,storage_account_key,storage_account_name,connection_string,container_name,targetContainerName,rawDirectory,songContainerName,processedDirectory,songDirectory,song_df,'songs')):
            print("Data Uploaded successfully")
            shutil.move(file,processedDirectory)
            blob_service_client=BlobServiceClient.from_connection_string(connection_string)
            blob_service = blob_service_client.get_blob_client(container=targetContainerName,blob='jsonfile_'+file.split('/')[-1].split('_')[2]+'_'+file.split('/')[-1].split('_')[3])
            jsonfile=json.dumps(jsonfile)
            blob_service.upload_blob(jsonfile)
            deletedFile=file.split('/')[-1]
            blob_service_client_to_delete=BlobServiceClient.from_connection_string(connection_string)
            deletefilecontainerclient=blob_service_client_to_delete.get_container_client(container_name)
            deletefilecontainerclient.delete_blob(deletedFile)

            print("Source file deleted successfully")



os.chdir(rawDirectory)
#print(os.getcwd())
cwd=os.getcwd()
files = os.listdir()
#print(files)

for file in files:
    if file.endswith('.json') and file.startswith('spotify'):
        #print(file)
        transformation(cwd+'/'+file,sourcepath,storage_account_key,storage_account_name,connection_string,container_name,targetContainerName,rawDirectory,artistContainerName,albumContainerName,songContainerName,processedDirectory,albumDirectory,artistDirectory,songDirectory)