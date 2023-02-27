from gh_api.export import to_parquet
import importlib_resources as resources
from pathlib import Path

p_schema = resources.files("gh_api") / "schemas"

for schema in p_schema.glob("*.yml"):
    if schema.name == "users.yml" or schema.name == "commits.yml": continue
    for repo in Path(".").glob("tidyverse*"):
        name = schema.with_suffix("").name
