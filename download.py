import pyarrow as pa
import json
import jq

from gh_api import extractors as ext
from gh_api.gh_api import GithubApiSession
from pyarrow import parquet
from pathlib import Path
from functools import partial

def fetch_and_save(mod, fname, *args, **kwargs):
    data = mod.fetch(*args, **kwargs)
    cleaned = mod.clean(data)

    #table = pa.table(cleaned)
    #parquet.write_table(table, fname)
    Path(fname).parent.mkdir(exist_ok=True, parents=True)

    to_ndjson(data, str(fname) + "--raw.ndjson")
    to_ndjson(cleaned, str(fname) + ".ndjson")

    return cleaned


def to_ndjson(d, outname):
    if isinstance(d, (str, Path)):
        d = json.load(open(d))

    with open(outname, "w") as f:
        for entry in d:
            json.dump(entry, f)
            f.write("\n")


def get_name(owner, name, root, fname):
    return Path(root) / f"{owner}+{name}" / fname

def dump_repo(owner="machow", name="siuba"):
    from dotenv import load_dotenv

    load_dotenv()

    p_results = Path("results2")

    f_name = partial(get_name, owner, name, p_results)

    repository = fetch_and_save(ext.repository, f_name("repository"), owner=owner, name=name)
    ##stargazers = fetch_and_save(ext.stargazers, f_name("stargazers"), owner=owner, name=name)
    ##labels = fetch_and_save(ext.labels, f_name("labels"), owner=owner, name=name)

    # repos with no default_branch have no commits. This screws up our current
    # pagination approach :/. So we check for a branch first.
    if repository[0]["default_branch"] is not None:
        commits = fetch_and_save(ext.commits, f_name("commits"), owner=owner, name=name)

    ##issues = fetch_and_save(ext.issues, f_name("issues"), owner=owner, name=name)
    ##pr_issues = fetch_and_save(ext.pr_issues, f_name("issues_pr"), owner=owner, name=name)

    ### get all issue ids (including PR ids) ----
    ##all_issue_ids = [issue["id"] for issue in issues + pr_issues]
    ##pr_ids = [issue["id"] for issue in pr_issues]

    ### enriched pr data ----
    ##fetch_and_save(ext.prs, f_name("pull_requests"), pr_ids)

    ### extract timeline data ----
    ##fetch_and_save(ext.issue_comments, f_name("issue_comments"), all_issue_ids)
    ##fetch_and_save(ext.issue_labels, f_name("issue_labels"), all_issue_ids)
    ##fetch_and_save(ext.issue_events, f_name("issue_events"), all_issue_ids)


if __name__ == "__main__":
    from datetime import datetime
    from gh_api.misc import fetch_owner_repos
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
