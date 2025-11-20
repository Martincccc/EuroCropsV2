import tempfile
tempfile.tempdir = '/scratch/clamart/'

import os
import re
import time
import requests
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

class _LinkExtractor(HTMLParser):
    """Tiny HTML link extractor for <a href="..."> tags."""
    def __init__(self):
        super().__init__()
        self.hrefs = []
    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            for k, v in attrs:
                if k.lower() == "href" and v:
                    self.hrefs.append(v)

def _list_http_directory(base_url,region_list,year_list, extension=".parquet"):
    """
    Fetch an HTTP(S) directory index and return filenames matching extension.
    """
    # Ensure it looks like a directory
    if not base_url.endswith("/"):
        base_url = base_url + "/"
    # Some servers are picky without a UA
    headers = {"User-Agent": "python-requests/ftp-migrator"}
    resp = requests.get(base_url, headers=headers, timeout=60)
    resp.raise_for_status()
    parser = _LinkExtractor()
    parser.feed(resp.text)
    # Keep only same-dir files (no parent links), with desired extension
    files = []
    for href in parser.hrefs:
        # Ignore parent dir and subdirectories
        if href in ("../", "./"):
            continue
        # Convert relative links to absolute to inspect path safely
        abs_url = urljoin(base_url, href)
        # Only capture items that appear to be files in this directory
        # (i.e., href not ending with '/' and ending with the extension)
        path = urlparse(abs_url).path
        name = os.path.basename(path)
        if name and not name.endswith("/") and name.lower().endswith(extension.lower()):
            if region_list==['all']:
                nutsok = True
            elif name.split('.')[0].split('_')[0] in region_list:
                nutsok = True
            else:
                nutsok = False
            if year_list==['all']:
                yearok = True
            elif name.split('.')[0].split('_')[1] in [str(y) for y in year_list]:
                yearok = True
            else:
                yearok = False
            if nutsok and yearok:
                files.append(name)
    # De-dup (some indexes repeat links)
    print(files)

    return sorted(set(files))

def _stream_download(url, local_path, chunk=8192):
    headers = {"User-Agent": "python-requests/ftp-migrator"}
    with requests.get(url, stream=True, headers=headers, timeout=300) as r:
        r.raise_for_status()
        # Write atomically: download to tmp then move
        tmp_path = local_path + ".part"
        with open(tmp_path, "wb") as f:
            for chunk_bytes in r.iter_content(chunk_size=chunk):
                if chunk_bytes:
                    f.write(chunk_bytes)
        os.replace(tmp_path, local_path)

def download_http_files(conf):
    """
    Download files from an HTTP(S) directory that exposes an index page.

    - If download_all=True, downloads all *.parquet files listed in the directory.
    - Else, downloads only files in file_list (each item may omit the .parquet extension).
    """

    base_url    =conf.url['ftp_download_url']
    local_dir   =conf.paths['fastio_dir']
    region_list =conf.region_list
    year_list   =conf.year_list

    t0 = time.time()
    os.makedirs(local_dir, exist_ok=True)

    # Normalize base_url to dir form
    if not base_url.endswith("/"):
        base_url = base_url + "/"

    target_files = _list_http_directory(base_url,region_list,year_list, extension=".parquet")
    if not target_files:
        print("No .parquet files found at the directory index.")
        return
    print(f"Found {len(target_files)} .parquet files. Starting download...")
    # else:
    #     if not file_list:
    #         raise ValueError("file_list must be provided if download_all is False")
    #     # Allow entries without extension and normalize to .parquet
    #     target_files = [f if f.lower().endswith(".parquet") else f + ".parquet" for f in file_list]
    #     print(f"Preparing to download {len(target_files)} files...")

    downloaded = 0
    skipped = 0
    for filename in target_files:
        file_url = urljoin(base_url, filename)
        local_path = os.path.join(local_dir, filename)

        if os.path.exists(local_path):
            print(f"Skipping (exists): {filename}")
            skipped += 1
            continue

        print(f"Downloading {file_url} ...")
        try:
            _stream_download(file_url, local_path)
            print(f"Saved to {local_path}")
            downloaded += 1
        except requests.HTTPError as e:
            # Keep going on 404/5xx but report clearly
            print(f"Failed ({e.response.status_code}) for {filename}: {e}")
        except Exception as e:
            print(f"Failed for {filename}: {e}")

    dt = time.time() - t0
    print(f"Done. Downloaded: {downloaded}, Skipped: {skipped}, Total: {len(target_files)} in {dt:.1f}s.")

def download(config):
    t1=time.time()
    print("Downloading files from HTTP(S) directory...")
    download_http_files(config)
    t2=time.time()
    print('-----> exec time = %.2fmn'%((t2-t1)/60))