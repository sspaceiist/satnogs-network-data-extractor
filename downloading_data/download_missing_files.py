import json
import os
from pathlib import Path
import requests
from urllib.parse import urlparse

# Configuration
SOURCE_DIR = "/mnt/is1-health"
JSONL_FILE = "raw_data.jsonl"
DOWNLOAD_DIR = "/mnt/is1-health/demodulated"  # Adjust as needed

def get_existing_files(directory):
    """Get all files in the directory."""
    if not os.path.exists(directory):
        return set()
    return set(os.listdir(directory))

def count_missing_files_and_return_urls(jsonl_file, target_dir):
    """Count how many files are missing."""
    existing_files = get_existing_files(target_dir)
    missing_count = 0
    missing_files_urls = {}
    with open(jsonl_file, 'r') as f:
        for line in f:
            data = json.loads(line)
            if data["demoddata"]:
                i=0
                for demod in data["demoddata"]:
                    url = demod["payload_demod"]
                    parsed_url = urlparse(url)
                    filename = os.path.basename(parsed_url.path)
                    saved_filename = str(data["id"]) + "_" + str(i) + "_" + filename
                    if saved_filename not in existing_files:
                        missing_files_urls[saved_filename] = url
                        missing_count += 1
                    i += 1
    print(f"{missing_count} files missing and {len(existing_files)} files present in {target_dir}")
    return missing_files_urls

def download_missing_files(jsonl_file, download_dir):
    """Download missing files from the provided URLs."""
    missing_files = count_missing_files_and_return_urls(jsonl_file, download_dir)
    print(f"Downloading {len(missing_files)} missing files to {download_dir}...")
    Path(download_dir).mkdir(parents=True, exist_ok=True)
    for filename, url in missing_files.items():
        try:
            response = requests.get(url)
            if response.status_code == 200:
                out_path = os.path.join(download_dir, filename)
                with open(out_path, "wb") as f:
                    f.write(response.content)
            elif response.status_code != 200:
                if response.status_code == 429:
                    wait = int(response.headers.get("Retry-After", 10)) + 10
                    print(f"Rate limited. Waiting for {wait} seconds before retrying...")
                    time.sleep(wait)
                    print("Retrying now...")
                    download_missing_files(jsonl_file, download_dir)
                print(f"Failed to download {url}, status code: {response.status_code}")
        except Exception as e:
            print(f"Error downloading {url}: {e}")


if __name__ == "__main__":
    download_missing_files(JSONL_FILE, DOWNLOAD_DIR)