import jq
from gh_api.gh_api import GithubApiSession

query = """
  query commitsQuery($owner: String!, $name: String!, $cursor: String) {
    repository(owner: $owner, name: $name) {
        id
        defaultBranchRef {
          target {
            ... on Commit {
              history(first: 100, after: $cursor) {
                nodes {
                  oid
                  repository {
                    id
                  }
                  author {
                    email
                    name
                    date
                  }
                  committer {
                    email
                    name
                    date
                  }
                  message
                }
                pageInfo {
                  endCursor
                  hasNextPage
                }
              }
            }
          }
        }
      }
    }
"""

def fetch(owner="tidyverse", name="dplyr", api_key=None):
    gh = GithubApiSession(api_key)

    return gh.paginated_query(
        query,
        cursor_path="repository.defaultBranchRef.target.history.pageInfo",
        variables = dict(
            owner=owner,
            name=name
        )
    )


def clean(data):
    q = """
        .[]
        | .repository.defaultBranchRef.target.history.nodes[]
        | {
            sha: .oid,
            repository_id: .repository.id,
            author_email: .author.email,
            committer_email: .committer.email,
            author_name: .author.name,
            author_date: .author.date,
            committer_name: .committer.name,
            committer_date: .committer.date,
            message: .message
          }
    """
    return jq.compile(q).input(data).all()

