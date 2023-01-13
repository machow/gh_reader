import jq
from .gh_api import GithubApiSession

def fetch_owner_repos(owner):
    gh = GithubApiSession()
    query = """
        query MyQuery($owner: String!, $cursor: String) {
          organization(login: $owner) {
            repositories(first: 10, after: $cursor) {
              nodes {
                nameWithOwner
              }
              pageInfo {
                endCursor
                hasNextPage
              }
            }
          }
        }
    """
    repos_raw = gh.paginated_query(
        query,
        cursor_path="organization.repositories.pageInfo",
        variables=dict(owner=owner)
    )

    return jq.compile(".[] | .organization.repositories.nodes[] | .nameWithOwner").input(repos_raw).all()

