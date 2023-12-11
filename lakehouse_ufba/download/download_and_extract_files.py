import os
from collections import namedtuple

import py7zr
from prefect import flow, task
from prefect.tasks import task_input_hash
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


@task(cache_key_fn=task_input_hash)
def get_gauth():
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)
    return drive    


@task@task(cache_key_fn=task_input_hash)
def download_file_from_drive(gauth, id_, save_as) -> None:    
    file = gauth.CreateFile({"id": id_})
    file.GetContentFile(save_as)
    
    
@task@task(cache_key_fn=task_input_hash)
def extract_7z(file_7z):
    with py7zr.SevenZipFile(file_7z, "r") as archive:
        archive.extractall(path=os.path.dirname(file_7z))
    
    
@flow  
def download_and_extact():
    drive = get_gauth()
    
    FilesDrive = namedtuple("FilesDrive", ["id_", "path"])
    
    files = [
        FilesDrive(os.environ["SOCIOECO"], "./data/raw/socioeconomico/socioeconomico.7z"),
        FilesDrive(os.environ["ACADEMICO"], "./data/raw/academico/ufba_academica_0321.7z"),
    ]
    
    for file in files:
        download_file_from_drive(gauth=drive, id_=file.id_, save_as=file.path)    
        extract_7z(file.path)
        

if __name__ == "__main__":
    download_and_extact()