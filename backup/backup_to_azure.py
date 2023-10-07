import os
import sys
import json
import hashlib

def detail_files_recursively(directory):
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
    
    return file_manifest
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("script invocation requires a path argument")
    else:
        directory_to_list = sys.argv[1]
        local_manifest = detail_files_recursively(directory_to_list)
        #print(json_obj)
        local_manifest_path = str(directory_to_list+"\manifest.json")
        with open(local_manifest_path, "w") as manifest_file:
            json.dump(local_manifest, manifest_file, indent=4)
        #local_manifest_json = local_manifest #json.load(local_manifest)
        # fetch remote manifest? or derive another one from contents of remote?
        # probably easier to fetch
        # probably more accurate to derive
        with open("D:\\automato\\manifest.json", "r") as local_manifest:
            local_manifest_json = json.load(local_manifest)
        
        with open("D:\\Code\\simple_automation\\backup\\remote_manifest.json", "r") as remote_manifest:
            remote_manifest_json = json.load(remote_manifest)
        #print(remote_manifest_json)
        #dunno if this works
        #compare_attribute = "object_id"
        #remote_hash_values = set(obj[compare_attribute] for obj in remote_manifest_json)
        #print(str(remote_hash_values))
        objects_missing_from_remote = {key:value for key, value in local_manifest_json.items() if key not in remote_manifest_json}
        #local_manifest_dict = {obj["object_id"]: obj for obj in local_manifest_list}
        #print(remote_manifest_list)
        print(objects_missing_from_remote)
        #print(local_manifest_json)
        