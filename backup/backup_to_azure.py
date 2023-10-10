import os
import sys
import json
import hashlib

from dotenv import load_dotenv

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

load_dotenv()

def generate_local_manifest(directory):
    file_manifest = {}
    root_dir_len = len(directory)+1
    try:
        for root, _, files in os.walk(directory):
            for file in files:
                os_path = os.path.join(root,file)
                os_path_flat = os_path[root_dir_len:]
                modified_time = os.path.getmtime(os_path)
                created_time = os.path.getctime(os_path)
                string_to_hash = str(os_path_flat) + str(created_time) + str(modified_time)
                hash_object = hashlib.sha256()
                hash_object.update(string_to_hash.encode("utf-8"))
                hashed_str = hash_object.hexdigest()
                
                file_json = {
                    "local_path": os_path,
                    "path": os_path_flat,
                    "create_time": str(created_time),
                    "modified_time": str(modified_time)
                }
                
                #print (os_path_flat + " ------ " + hashed_str)
                file_manifest[hashed_str] = file_json
            
    except OSError as e:
        print(f"Error: {e}")
    
    local_manifest_path = str(directory_to_list+"\manifest.json")
    with open(local_manifest_path, "w") as manifest_file:
        json.dump(file_manifest, manifest_file, indent=4)
        
    return local_manifest_path
    
def pull_manifest_from_azure(directory, container_target):
    connection_string = os.getenv('AZURE_CONNECTION_STRING')
    container_name = container_target
    blob_name = "manifest.json"
    local_destination = directory + "\\remote_manifest.json"
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    
    with open(local_destination, "wb") as blob_target:
        blob_target.write(blob_client.download_blob().readall())
        
    return local_destination
    
#def upload_local_file_to_azure(local_file_path, remote_target_name, target_container, remote_metadata):
def upload_local_file_to_azure(local_file_path, remote_target_name, target_container):
    print (f"uploading local file '{local_file_path}' to target '{remote_target_name}' in container '{target_container}'. overwriting any existing file in remote with matching path")
    blob_service_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_CONNECTION_STRING'))
    container_client = blob_service_client.get_container_client(target_container)
    with open(local_file_path, "rb") as data:
        blob_client = container_client.get_blob_client(remote_target_name)
        #blob_client.upload_blob(data, metadata=remote_metadata, overwrite=True)
        blob_client.upload_blob(data, overwrite=True)
    
    
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("script invocation requires a path argument for local path and target azure container")
    else:
        directory_to_list = sys.argv[1]
        associated_container = sys.argv[2]
        local_manifest_path = generate_local_manifest(directory_to_list)
        remote_manifest_path = pull_manifest_from_azure(directory_to_list, associated_container)
        
        with open(local_manifest_path, "r") as local_manifest:
            local_manifest_json = json.load(local_manifest)
        
        with open(remote_manifest_path, "r") as remote_manifest:
            remote_manifest_json = json.load(remote_manifest)

        objects_missing_from_remote = {key:value for key, value in local_manifest_json.items() if key not in remote_manifest_json}
        
        try:
            for key, value in objects_missing_from_remote.items():
                if (value["path"] == "manifest.json" or value["path"] == "remote_manifest.json"):
                    continue
                print (f" found following new file, preparing it for upload. \n key :: {key} \n value :: {str(value)}")
                
                # omitting this for now, I don't think I need it since I decided to go manifest/dict approach
                #captured_metadata = {
                #    "path": os_path_flat,
                #    "create_time": str(created_time),
                #    "modified_time": str(modified_time)
                #}
                upload_local_file_to_azure(value["local_path"], value["path"], associated_container)
        finally:
            #update manifest after successful upload
            upload_local_file_to_azure(local_manifest_path, "manifest.json", associated_container)
            
            
        