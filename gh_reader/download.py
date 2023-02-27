import pyarrow as pa
import json
import jq

from gh_reader import extractors as ext
from gh_reader.gh_api import GithubApiSession
from gh_reader.export import to_parquet, to_ndjson
from pyarrow import parquet
from pathlib import Path
from functools import partial


class Downloader:
    skip_parquet = {"issue_events"}

    def __init__(self, root, api_key=None):
        self.root = root
        self.api_key = api_key

    @staticmethod
    def get_name(owner, name, root, fname):
        return Path(root) / f"{owner}+{name}" / fname

    def fetch_and_save(self, mod, fname, *args, **kwargs):
        # TODO: getting name like this is very hacky
        name = mod.__name__.split(".")[-1]

        print(f"Extracting: {name}")

        data = mod.fetch(*args, **kwargs, api_key=self.api_key)
        cleaned = mod.clean(data)

        #table = pa.table(cleaned)
        #parquet.write_table(table, fname)
        self.mkdir(Path(fname).parent, exist_ok=True, parents=True)

        to_ndjson(data, self.open(str(fname) + "--raw.ndjson"))
        to_ndjson(cleaned, self.open(str(fname) + ".ndjson"))

        if name not in self.skip_parquet:
            to_parquet(name, cleaned, self.open(str(fname) + ".parquet", mode="wb"))

        return cleaned

    def open(self, fname, mode = "w"):
        return open(fname, mode)

    def mkdir(self, fname, exist_ok=True, parents=True):
        Path(fname).mkdir(exist_ok=exist_ok, parents=parents)

    def dump_repo(self, owner="machow", name="siuba"):
        f_name = partial(self.get_name, owner, name, self.root)

        repository = self.fetch_and_save(ext.repository, f_name("repository"), owner=owner, name=name)
        stargazers = self.fetch_and_save(ext.stargazers, f_name("stargazers"), owner=owner, name=name)
        labels = self.fetch_and_save(ext.labels, f_name("labels"), owner=owner, name=name)

        # repos with no default_branch have no commits. This screws up our current
        # pagination approach :/. So we check for a branch first.
        if repository[0]["default_branch"] is not None:
            commits = self.fetch_and_save(ext.commits, f_name("commits"), owner=owner, name=name)

        issues = self.fetch_and_save(ext.issues, f_name("issues"), owner=owner, name=name)
        issues_pr = self.fetch_and_save(ext.issues_pr, f_name("issues_pr"), owner=owner, name=name)

        # get all issue ids (including PR ids) ----
        all_issue_ids = [issue["id"] for issue in issues + issues_pr]
        pr_ids = [issue["id"] for issue in issues_pr]

        # enriched pr data ----
        self.fetch_and_save(ext.pull_requests, f_name("pull_requests"), pr_ids)

        # extract timeline data ----
        self.fetch_and_save(ext.issue_comments, f_name("issue_comments"), all_issue_ids)
        self.fetch_and_save(ext.issue_labels, f_name("issue_labels"), all_issue_ids)
        self.fetch_and_save(ext.issue_events, f_name("issue_events"), all_issue_ids)


if __name__ == "__main__":
    from datetime import datetime
    from gh_reader.misc import fetch_owner_repos
    from dotenv import load_dotenv

    load_dotenv()

    gh = GithubApiSession()
    repos = fetch_owner_repos("CodeForPhilly")
    #repos = [
    ##"rstudio/DT",
    ##"ramnathv/htmlwidgets",
    ##"r-lib/fastmap",
    ##"r-lib/later",

    ##"ropensci/plotly",
    ##"rstudio/gradethis",

    ##"r-lib/ymlthis",
    ##"rstudio/bslib",
    ##"r-lib/cachem",
    ##"rstudio/chromote",
    ##"rstudio/crosstalk",
    ##"rstudio/flexdashboard",
    ##"rstudio/gridlayout",
    #"rstudio/gt",
    #"rstudio/htmltools",
    #"rstudio/httpuv",
    #"rstudio/jquerylib",
    #"rstudio/leaflet",
    #"rstudio/leaflet.providers",
    #"rstudio/learnr",
    #"rstudio/plumber",
    #"rstudio/pool",
    #"rstudio/profvis",
    #"rstudio/promises",
    #"rstudio/r2d3",
    #"rstudio/reactlog",
    #"rstudio/remarker",
    #"rstudio/rmarkdown",
    #"rstudio/sass",
    #"rstudio/shiny",
    #"rstudio/shiny-server",
    #"rstudio/shiny-examples",
    #"rstudio/shinybootstrap2",
    #"rstudio/shinycannon",
    #"rstudio/shinycoreci-apps",
    #"rstudio/shinycoreci",
    #"rstudio/shinydashboard",
    #"rstudio/shinyloadtest",
    #"rstudio/shinymeta",
    #"rstudio/shinytest",
    #"rstudio/shinythemes",
    #"rstudio/shinyvalidate",
    #"rstudio/sortable",
    #"rstudio/swagger",
    #"rstudio/thematic",
    #"rstudio/websocket",
    #"rstudio/webdriver",
    #"schloerke/shinyjster"
    #]

    # TODO: start with tidyverse/dbplyr
    # handle rate limiting
    for ii, repo in enumerate(repos):
        owner, name = repo.split("/")
        print(f"\n\n\ndumping {ii}: {repo} ------------------------\n")
        print("time: ", str(datetime.now()))
        dump_repo(owner, name)
