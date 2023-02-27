import tempfile
import os

from gh_reader.download import Downloader

def test_downloader():
    with tempfile.TemporaryDirectory() as tmp_dir:
        downloader = Downloader(tmp_dir, api_key=os.environ["TESTS_GITHUB_TOKEN"])
        downloader.dump_repo("tidyverse", "tidytemplate")


