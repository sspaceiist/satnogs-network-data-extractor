import os
import requests
from urllib.parse import urlparse

SAT_ID = 51657 # This id is for InspireSat-1, Change this to the desired satellite ID
BASE_URL = "https://network.satnogs.org/api/observations/"
OUTDIR = f"satnogs_{SAT_ID}"
os.makedirs(OUTDIR, exist_ok=True)

def get_next_page_url(response):
    if "next" in response.links:
        return response.links["next"]["url"]
    return None


def download_observations(url):
    response = requests.get(url)
    data = response.json()
    print(get_next_page_url(response))

if __name__ == "__main__":
    download_observations(f"{BASE_URL}?norad_cat_id={SAT_ID}") #for InspireSat-1
