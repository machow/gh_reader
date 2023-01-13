import jq

from gh_api.gh_api import GithubApiSession

# TODO: currently only fetches top 100 comments
query_issue_labels = """
    query issueLabels($node_ids: [ID!]!) {
      nodes(ids: $node_ids) {
        ... on Issue {
          id

          labels(first: 25) {
            nodes {
              label_id: id
            }
            pageInfo {
              endCursor
              hasNextPage
            }
          }
        }

        ... on PullRequest {
          id

          labels(first: 25) {
            nodes {
              label_id: id
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

    # TODO: consolidate

    # from https://stackoverflow.com/a/312464/1144523
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch(ids, api_key=None):
    gh = GithubApiSession(api_key)

    all_data = []
    for ids_chunk in _chunks(ids, 100):

        crnt_data = gh.query(
            query_issue_labels,
            node_ids = ids_chunk
        )

        all_data.append(crnt_data["data"])

    return all_data


def clean(data):
    q = ".[] | .nodes[] | . as $issue | .labels.nodes[] | {issue_id: $issue.id} + ."

    return jq.compile(q).input(data).all()
