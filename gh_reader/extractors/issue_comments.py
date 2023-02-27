import jq
from gh_reader.gh_api import GithubApiSession

# TODO: currently only fetches top 100 comments
query_issue_comments = """
    fragment issueCommentData on IssueComment {
      id
      issue { id }
      author {
        ... on User { id }
        ... on Bot { id }
        ... on Organization { id }
      }
      body
      created_at: createdAt
      updated_at: updatedAt
    }

    query issueComments($node_ids: [ID!]!) {
      nodes(ids: $node_ids) {
        ... on Issue {
          comments(first: 100) {
            nodes {
              ... issueCommentData
            }
            pageInfo {
              endCursor
              hasNextPage
            }
          }
        }
        ... on PullRequest {
          comments(first: 100) {
            nodes {
              ... issueCommentData
            }
            pageInfo {
              endCursor
              hasNextPage
            }
          }
        }
      }
    }

"""


def _chunks(lst, n):
    """Yield successive n-sized chunks from lst."""

    # from https://stackoverflow.com/a/312464/1144523
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch(ids, api_key=None):
    gh = GithubApiSession(api_key)

    all_data = []
    for ids_chunk in _chunks(ids, 100):

        crnt_data = gh.query(
            query_issue_comments,
            node_ids = ids_chunk
        )

        all_data.append(crnt_data["data"])

    return all_data


def clean(data):
    q = """
    .[]| .nodes[] | .comments.nodes[] | {
      id: .id,
      issue_id: .issue.id,
      user_id: .author.id,
      body: .body,
      created_at: .created_at,
      updated_at: .updated_at
    }
    """
    return jq.compile(q).input(data).all()
