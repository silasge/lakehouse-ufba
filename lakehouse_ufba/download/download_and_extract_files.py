import os
from collections import namedtuple

import py7zr
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

from lakehouse_ufba.utils import log


def get_gauth():
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)
    return drive    


def download_file_from_drive(gauth, id_, save_as) -> None:    
    file = gauth.CreateFile({"id": id_})
    file.GetContentFile(save_as)
    
    
def extract_7z(file_7z):
    with py7zr.SevenZipFile(file_7z, "r") as archive:
        archive.extractall(path=os.path.dirname(file_7z))
    
@log(log_file="logs/download_and_extract.log")    
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
        