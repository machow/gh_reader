from gh_reader.gh_api import GithubApiSession
import jq

query_issues = """
    fragment issueData on Issue {
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

    query repoIssues($owner: String!, $name: String!, $since: DateTime, $cursor: String) {
      repository(owner: $owner, name: $name) {
        issues(first: 100, after: $cursor, filterBy: { since: $since }) {
          nodes {
            ... issueData
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
      }
    }

"""


def fetch(owner="tidyverse", name="dplyr", since = None, api_key=None):
    """Fetch issues for a repository.

    Notes
    -----

    The since parameter uses the issues updatedAt field, so may return issues
    that were created earlier and subsequently updated.

    """

    gh = GithubApiSession(api_key)

    return gh.paginated_query(
        query_issues,
        cursor_path="repository.issues.pageInfo",
        next_key="endCursor",
        next_check_key="hasNextPage",
        cursor_variable="cursor",
        variables=dict(
            owner=owner,
            name=name,
            since = since
        )
    )


def clean(data):
    q = """
        .[] | .repository.issues.nodes[] | {
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
