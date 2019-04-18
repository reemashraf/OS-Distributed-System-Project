from pathlib import Path
import os
import math

##format filename correctly

def get_chunck_name_by_number(number , file_path):
    file_name = Path(file_path).stem
    print("file path" , file_path)
    print(file_name)
    file_size = get_file_size(file_path)
    number_of_digits = len(str(math.ceil(file_size / CHUNK_SIZE)))
    return  str(number).zfill(number_of_digits) + "_" + str(file_name) + ".chunck"


def get_file_size(filename):
    fileobject = open(filename, 'rb')
    fileobject.seek(0,2) # move the cursor to the end of the file
    size = fileobject.tell()
    fileobject.close()
    return size


def slice_file(file_path , CHUNK_SIZE):
    file_size = get_file_size(file_path)
    file_name = Path(file_path).stem
    number_of_chuncks =  math.ceil(file_size / CHUNK_SIZE)
    number_of_digits = len(str(number_of_chuncks))
    file_number = 1
    file_path_split = file_path.replace(file_name +".mp4" , " ")
    print(file_path)
    with open(file_path , 'rb') as f:
        chunk = f.read(CHUNK_SIZE)
        while chunk:
            print(file_path)
            with open(file_path_split + str(file_number).zfill(number_of_digits) +"_"+str(file_name) + ".chunck" , 'wb') as chunk_file:
                chunk_file.write(chunk)
            file_number += 1
            chunk = f.read(CHUNK_SIZE)
    return number_of_chuncks

def deslice_file(file_path):
    file_number = 1
    read_data = []
    file_name = Path(file_path).stem
    print("file_name" ,file_name)
    final = file_name + "dechunk" + ".mp4"
    directory = file_path.replace("/" + file_name +".mp4", "/")
    print("-"+directory+"-")
    for filename in os.listdir(directory):
        if filename.endswith(".chunck"):
            chunk = open( directory +filename , 'rb')
            print(filename)
            read_data += chunk.read()
    read_data = bytes(read_data)
    f = open( directory + "/"+final , 'wb')
    f.write(read_data)
    
file_size = (get_file_size("vid_1.mp4"))
print(file_size)
CHUNK_SIZE = 64*1024
number_of_chuncks = math.ceil(file_size / CHUNK_SIZE)
file_path = Path( "./"+ "vid_2.mp4" )
# print(number_of_chuncks)
#slice_file("./reem/vid_1.mp4" , 64*1024 )
# deslice_file()
#print(get_chunck_name_by_number( 16 , "./reem/trial.mp4"))
#get_chunck_name_by_number, slice_file tested, deslice tested
deslice_file("./reem/vid_1.mp4")