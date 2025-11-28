import os
import requests
from urllib.parse import urlparse
import time

SAT_ID = 51657 # This id is for InspireSat-1, Change this to the desired satellite ID
BASE_URL = "https://network.satnogs.org/api/observations/"
OUTDIR = f"satnogs_{SAT_ID}"
os.makedirs(OUTDIR, exist_ok=True)
os.makedirs(os.path.join(OUTDIR, "audios"), exist_ok=True)
os.makedirs(os.path.join(OUTDIR, "demodulated"), exist_ok=True)

def get_next_page_url(response):
    if "next" in response.links:
        return response.links["next"]["url"]
    return None

def log(message):
    with open(os.path.join(OUTDIR, "log.txt"), "a") as f:
        f.write(message + "\n")

def logerror(message):
    with open(os.path.join(OUTDIR, "error_log.txt"), "a") as f:
        f.write(message + "\n")

def logobs(message):
    with open(os.path.join(OUTDIR, "observations_log.txt"), "a") as f:
        f.write(message + "\n")

def download_observations_and_return_next(url):
    start_time = time.time()
    response = requests.get(url)
    data = response.json()
    for observation in data:
        obs_id = observation["id"]
        audio_file = observation["payload"]
        if audio_file:
            try:
                file_response = requests.get(audio_file)
                parsed_url = urlparse(audio_file)
                filename = os.path.basename(parsed_url.path)
                out_path = os.path.join(OUTDIR,"audios", f"{obs_id}_{filename}")
                with open(out_path, "wb") as f:
                    f.write(file_response.content)
                #log(f"Downloaded raw signal of observation {obs_id} to {out_path}")
            except:
                logerror(audio_file)
        demod_file_urls = observation["demoddata"]
        demod_data_index=0
        for demod_file_url in demod_file_urls:
            try:
                file_response = requests.get(demod_file_url["payload_demod"])
                parsed_url = urlparse(demod_file_url["payload_demod"])
                filename = os.path.basename(parsed_url.path)
                out_path = os.path.join(OUTDIR,"demodulated", f"{obs_id}_{demod_data_index}_{filename}")
                with open(out_path, "wb") as f:
                    f.write(file_response.content)
                #log(f"Downloaded demodulated data index {demod_data_index} of observation {obs_id} to {out_path}")
                demod_data_index += 1
            except:
                logerror(demod_file_url["payload_demod"])
        logobs(f"Finished downloading observation {obs_id}")
    end_time = time.time()
    elapsed_time = end_time - start_time
    log(f"Completed downloading all files from page: {url} in {elapsed_time:.2f} seconds")
    next_url = get_next_page_url(response)
    log(f"Next page URL: {next_url} \n\n\n")
    return next_url

if __name__ == "__main__":
    # current_url = f"{BASE_URL}?norad_cat_id={SAT_ID}"
    current_url = "https://network.satnogs.org/api/observations/?cursor=cD0yMDI1LTA4LTI3KzA1JTNBNDQlM0ExMyUyQjAwJTNBMDA%3D&norad_cat_id=51657"
    next_url = download_observations_and_return_next(current_url) #for InspireSat-1
    while next_url:
        current_url = next_url
        next_url = download_observations_and_return_next(current_url)