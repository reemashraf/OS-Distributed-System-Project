from pathlib import Path
import os
import math

##format filename correctly

def get_chunck_name_by_number(number , file_path):
    file_size = get_file_size(file_path)
    number_of_digits = len(str(math.ceil(file_size / CHUNK_SIZE)))
    return   "./"+ str(number).zfill(number_of_digits) + "_" + file_path[:-4] + ".chunck"


def get_file_size(filename):
    fileobject = open(filename, 'rb')
    fileobject.seek(0,2) # move the cursor to the end of the file
    size = fileobject.tell()
    fileobject.close()
    return size


def slice_file(file_path , CHUNK_SIZE):
    file_size = get_file_size(file_path)
    number_of_chuncks =  math.ceil(file_size / CHUNK_SIZE)
    number_of_digits = len(str(number_of_chuncks))
    file_number = 1
    with open(file_path , 'rb') as f:
        chunk = f.read(CHUNK_SIZE)
        while chunk:
            with open(str(file_number).zfill(number_of_digits) +"_"+file_path[:-4] + ".chunck" , 'wb') as chunk_file:
                chunk_file.write(chunk)
            file_number += 1
            chunk = f.read(CHUNK_SIZE)
    return number_of_chuncks

def deslice_file():
    file_number = 1
    read_data = []
    final = "dechunk_4.mp4"
    directory = "./"
    for filename in os.listdir(directory):
        if filename.endswith(".chunck"):
            chunk = open( filename , 'rb')
            print(filename)
            read_data += chunk.read()
    read_data = bytes(read_data)
    f = open(final , 'wb')
    f.write(read_data)
    
file_size = (get_file_size("vid_1.mp4"))
print(file_size)
CHUNK_SIZE = 64*1024
number_of_chuncks = math.ceil(file_size / CHUNK_SIZE)
file_path = Path( "./"+ "vid_2.mp4" )
# print(number_of_chuncks)
# slice_file(file_path.name , 64*1024 ,  len(str(number_of_chuncks)))
# deslice_file()
