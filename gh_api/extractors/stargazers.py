import jq

from gh_api.gh_api import GithubApiSession

query_stargazers = """
   query stargazers($owner: String!, $name: String!, $cursor: String) {
     repository(name: $name, owner: $owner) {
       id
       stargazers(first: 100, after: $cursor) {
         edges {
           starredAt
           node {
             id
             login
           }
         }
         pageInfo {
           endCursor
           hasNextPage
         }
       }
     }
   }
"""


def fetch(owner="tidyverse", name="dplyr", api_key=None):
    gh = GithubApiSession(api_key)

    return gh.paginated_query(
        query_stargazers,
        cursor_path="repository.stargazers.pageInfo",
        next_key="endCursor",
        next_check_key="hasNextPage",
        cursor_variable="cursor",
        variables=dict(
            owner=owner,
            name=name
        )
    )


def clean(data):
    q = """
    .[]
    | .repository
    | . as $repo
    | .stargazers.edges[]
    | {
      repository_id: $repo.id,
      user_id: .node.id,
      user_login: .node.login,
      starred_at: .starredAt,
    }
    """
    return jq.compile(q).input(data).all()
