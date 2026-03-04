import config
import json
import os
import gzip
import zipfile

def read_json_file(file_path):
    """Reads a JSON file and yields its content as a dictionary."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        yield data

    except Exception as e:
        print("Error in func:", read_json_file.__name__, '\nError: ', e)

def read_binary_json_file(file_path):
    """Reads a JSON file and yields its content as a dictionary."""
    try:
        with open(file_path, 'rb', encoding='utf-8') as file:
            f=file.read().decode()
        data = json.loads(f)
        yield data
    except Exception as e:
        print("Error in func:", read_binary_json_file.__name__, '\nError: ', e)

def read_files_zip(path: str):
    '''
    generator func that takes dir path as input param in string
    and iterates over zipped files and yield results
    '''
    try:
        files = os.listdir(path)
        for file in files:
            filename = os.path.join(path, file)
            content = gzip.open(filename).read()
            yield json.loads(content)
    except Exception as e:    
        print("Error in func:", read_files_zip.__name__, '\nError: ', e)