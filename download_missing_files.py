import json
import os
from pathlib import Path
import requests
from urllib.parse import urlparse

# Configuration
SOURCE_DIR = "/mnt/is1-health"
JSONL_FILE = "raw_data.jsonl"
DOWNLOAD_DIR = "/mnt/is1-health"  # Adjust as needed

def get_existing_files(directory):
    """Get all files in the directory."""
    if not os.path.exists(directory):
        return set()
    return set(os.listdir(directory))

def get_files_from_jsonl(jsonl_file):
    """Extract filenames from JSONL file."""
    files = set()
    if not os.path.exists(jsonl_file):
        return files
    
    with open(jsonl_file, 'r') as f:
        for line in f:
            try:
                data = json.loads(line)
                # Adjust key based on your JSONL structure
                if 'filename' in data:
                    files.add(data['filename'])
                elif 'file' in data:
                    files.add(data['file'])
            except json.JSONDecodeError:
                continue
    return files

def find_missing_files():
    """Find files listed in JSONL but missing from directory."""
    existing = get_existing_files(SOURCE_DIR)
    in_jsonl = get_files_from_jsonl(JSONL_FILE)
    
    missing = in_jsonl - existing
    
    print(f"Files in {SOURCE_DIR}: {len(existing)}")
    print(f"Files in {JSONL_FILE}: {len(in_jsonl)}")
    print(f"Missing files: {len(missing)}")
    
    for filename in sorted(missing):
        print(f"  - {filename}")
    
    return missing

if __name__ == "__main__":
    with open(JSONL_FILE, 'r') as f:
        for line in f:
            data = json.loads(line)
            if data["payload"]:
                file_response = requests.get(data["payload"])
                parsed_url = urlparse(data["payload"])
                filename = os.path.basename(parsed_url.path)
                obs_id = data["id"]
                out_path = os.path.join(DOWNLOAD_DIR, "audios", f"{obs_id}_{filename}")
                with open(out_path, "wb") as f:
                    f.write(file_response.content)