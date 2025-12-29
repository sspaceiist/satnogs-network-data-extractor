# for fetching name(only name not files for files check extractor.py) of all files and data and creating the database


import os
import time
import requests
import json
from urllib.parse import urlparse

def get_next_page_url(response):
    if "next" in response.links:
        return response.links["next"]["url"]
    return None

def fetch_next_page_url(url):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch data from {url}, status code: {response.status_code}")
        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After")) + 10
            print(f"waiting for {wait} seconds before retrying...")
            time.sleep(wait)
            print("retrying now...")
            return fetch_next_page_url(url)
        return None
    data = response.json()
    with open("raw_data.jsonl", "a", encoding="utf-8") as f: #appending data to a jsonl file to prevent reading whole JSON again and again
        for obj in data:
            f.write(json.dumps(obj) + "\n")    
    next_url = get_next_page_url(response)
    return next_url


if __name__ == "__main__":
    current_url = "https://network.satnogs.org/api/observations/?cursor=cD0yMDI1LTAyLTIwKzE4JTNBMDglM0E1MiUyQjAwJTNBMDA%3D&norad_cat_id=51657" #in case of stopped dump in between and you have to restart it where it stopped just take the last url for next page in log.txt file and paste here and then re run the script
    next_url = fetch_next_page_url(current_url) #for InspireSat-1
    while next_url:
        current_url = next_url
        next_url = fetch_next_page_url(current_url)
        print(f"Next page URL: {current_url}")
    