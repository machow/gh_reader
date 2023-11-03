import jq
from gh_reader.gh_api import GithubApiSession


# Currently fetches only top 100 reactions
query_reactions = """
    fragment reactionData on Reaction {
      id
      content
      createdAt
      databaseId
      user {
        id
      }
    }

    query reactions($node_ids: [ID!]!) {
      nodes(ids: $node_ids) {
        ... on Issue {
          reactions(first: 100) {
            nodes {
              ... reactionData
            }
            pageInfo {
              endCursor
              hasNextPage
            }
          }
        }
        ... on PullRequest {
          reactions(first: 100) {
            nodes {
              ... reactionData
            }
            pageInfo {
              endCursor
              hasNextPage
            }
          }
        }
        ... on IssueComment {
          reactions(first: 100) {
            nodes {
              ... reactionData
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
            query_reactions,
            node_ids = ids_chunk
        )

        all_data.append(crnt_data["data"])

    return all_data


def clean(data):
    q = """.[] | .nodes[] | .reactions.nodes[] | {
      id: .id,
      user_id: .user.id,
      content: .content,
      created_at: .createdAt,
    }
    """

    return jq.compile(q).input(data).all()