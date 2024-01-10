import requests
import csv
from bs4 import BeautifulSoup as bs
import re
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from alive_progress import alive_bar


# List Level
levels = ["SD", "MI", "SMP", "MTS", "SMA", "MA", "SMK", "TK", "KB"]


# Collecting Kode Kabupaten
def get_kode_kab():
    cek = requests.get(
        "https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/400200/SD"
    )

    kode_kab = []
    soup = bs(cek.text, "lxml")
    item = soup.find("select", id="kode_kabupaten")
    hasil = item.find_all("option")
    for i in hasil:
        kode_kab.append(i["value"])

    return kode_kab


def create_urls(kode_list, level_list):
    url_list = []
    for kode in kode_list:
        for level in level_list:
            url = f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/{kode}/{level}"
            url_list.append(url)
    return url_list


def write_headers(file_success, file_failed):
    # Write the header row to the CSV file
    with open(file_success, "w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(
            ["NPSN", "School Name", "Href", "Alamat", "Latitude", "Longitude", "Url"]
        )

    with open(file_failed, "w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["url"])


def get_link_new(url):
    level = re.search(r"\w{2,3}$", url).group(0)

    # Define the file names
    file_success = f"school_data_{level}.csv"
    file_failed = f"school_data_{level}_failed.csv"

    # Write headers if file does not exist
    if not os.path.isfile(file_success) and not os.path.isfile(file_failed):
        write_headers(file_success, file_failed)

    # Open the files with the appropriate mode
    with open(
        file_success, "a", newline="", encoding="utf-8"
    ) as csv_file_success, open(
        file_failed, "a", newline="", encoding="utf-8"
    ) as csv_file_failed:
        # Create a CSV writer object
        csv_writer_success = csv.writer(csv_file_success)
        csv_writer_failed = csv.writer(csv_file_failed)

        try:
            html = requests.get(url, timeout=10)
            if html.status_code == 200:
                soup = bs(html.text, "lxml")
                scripts = soup.find_all("script")

                for i in scripts:
                    if "var colors" in i.text:
                        regex_pattern = r'<div class="no-margin">.*?NPSN : (.*?)<\/li>.*?<a href="(.*?)" target="_blank">(.*?)<\/a>.*?<b>Alamat<\/b> : (.*?)<\/li>.*?L\.latLng\((.*?),\s*(.*?)\)\)\.bindPopup'

                        # Find all matches using re.findall
                        matches = re.findall(regex_pattern, i.text, re.DOTALL)

                        # Extracted information
                        for match in matches:
                            npsn, href, school_name, alamat, latitude, longitude = match

                            # Write the data to the CSV file
                            csv_writer_success.writerow(
                                [
                                    npsn,
                                    school_name,
                                    href,
                                    alamat,
                                    latitude,
                                    longitude,
                                    url,
                                ]
                            )
                        status = "Success"

            else:
                csv_writer_failed.writerow([url])
                status = "Failed"
            time.sleep(2)

        except:
            csv_writer_failed.writerow([url])
            status = "Failed"
    return status


if __name__ == "__main__":
    print("Initiate get_kode_kab()!")
    kode_kab = get_kode_kab()
    print("Initiate create_urls()")
    url_list = create_urls(kode_kab, levels)

    print("Initiate Scraping!")
    with alive_bar(len(url_list)) as bar:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(get_link_new, url) for url in url_list]
            results = []
            for done in as_completed(futures):
                results.append(done)
                bar()

    print("Url Success : ", results.count("Success"))
    print("Url Failed : ", results.count("Failed"))
