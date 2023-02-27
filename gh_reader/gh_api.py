import requests
import os
import jq
import time

API_ENDPOINT = "https://api.github.com/graphql"


# API -------------------------------------------------------------------------

class GithubApiError(Exception):
    """Indicates an error from the github API."""

class GithubApiRateLimitError(GithubApiError):
    """Indicates an error from the github API."""

class GithubApiSession:
    """Query the github graphql API, with support for pagination."""

    def __init__(self, github_token=None, api_endpoint=API_ENDPOINT):
        """Initialize a github graphql API session.
        Args:
            github_token: a github api token. If none, taken from GITHUB_TOKEN env var.
            api_endpoint: url for the github graphql api endpoint.
        """

        self.session = requests.Session()
        self.api_endpoint = api_endpoint

        github_token = (
            os.environ["GITHUB_TOKEN"] if github_token is None else github_token
        )
        self.session.headers.update({
            "Authorization": f"token {github_token}",
            #"X-Github-Next-Global-ID": "1"
        })

    def query(self, q, **kwargs):
        """Execute a graphql query."""
        r = self.session.post(self.api_endpoint, json=dict(query=q, variables=kwargs))

        self.last_query = r

        if r.status_code == 401:
            r.raise_for_status()
        
        if r.status_code == 402 or r.status_code == 403:
            # TODO: handle retries using header if available, a retry wrapper
            print("\n\nHTTP 4xx BACK OFF -----------")
            print(r.headers)
            print(r.text)
            print("--------")
            time.sleep(120)
            r = self.session.post(self.api_endpoint, json=dict(query=q, variables=kwargs))

        if r.status_code != 200:
            raise GithubApiRateLimitError(r)
        data = r.json()
        self.validate_query_result(data)

        return data

    def paginated_query(
        self,
        q,
        cursor_path,
        next_key="endCursor",
        next_check_key="hasNextPage",
        start_cursor=None,
        variables=None,
        cursor_variable="cursor",
    ):
        """Execute a graphql query that involves pagination. Return all results."""

        variables = {} if variables is None else variables

        all_results = []

        next_cursor = start_cursor
        has_next_page = True
        while has_next_page:
            raw_data = self.query(q, **variables, **{cursor_variable: next_cursor})

            data = raw_data["data"]

            all_results.append(data)

            cursor_data = self._extract_path(cursor_path, data)
            next_cursor = cursor_data[next_key]
            has_next_page = cursor_data[next_check_key]


        return all_results

    @staticmethod
    def validate_query_result(data):
        """Validate whether graphql query was successful."""
        if data.get("errors"):
            raise GithubApiError(data)
        if data.get("message") and "exceeded a secondary rate limit" in data["message"]:
            raise GithubApiRateLimitError(data)

    @staticmethod
    def _extract_path(p, data):
        if callable(p):
            return p(data)

        entry = data
        for attr_name in p.split("."):
            entry = entry[attr_name]

        return entry

