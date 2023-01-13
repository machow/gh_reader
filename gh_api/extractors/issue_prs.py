import jq

from gh_api.gh_api import GithubApiSession

query_pull_requests = """
    fragment prIssueData on PullRequest {
      id
      milestone { id }
      repository { id }
      author {
        ... on User { id }
        ... on Bot { id }
        ... on Organization { id }
      }
      body
      closed_at: closedAt
      created_at: createdAt
      locked
      number
      state
      title
      updated_at: updatedAt
      type: __typename
    }

    query repoIssues($owner: String!, $name: String!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        pullRequests(first: 100, after: $cursor) {
          nodes {
            ... prIssueData
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
      }
    }

"""


def fetch(owner="tidyverse", name="dplyr"):
    gh = GithubApiSession()

    return gh.paginated_query(
        query_pull_requests,
        cursor_path="repository.pullRequests.pageInfo",
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
        | .repository.pullRequests.nodes[]
        | { 
          id: .id,
          milestone_id: .milestone.id,
          repository_id: .repository.id,
          user_id: .author.id,
          body: .body,
          closed_at: .closed_at,
          created_at: .created_at,
          locked: .locked,
          number: .number,
          state: .state,
          title: .title,
          updated_at: .updated_at,
          type: .type
        }
       
    """
    return jq.compile(q).input(data).all()

