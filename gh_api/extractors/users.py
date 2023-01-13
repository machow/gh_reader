import jq
from gh_api.gh_api import GithubApiSession

# TODO: currently only fetches top 100 comments
query = """
    query user($node_ids: [ID!]!) {
      nodes(ids: $node_ids) {
        ... on User {
          email
          bio
          createdAt
          id
          login
          updatedAt
          twitterUsername
        }
      }
    }

"""


def _chunks(lst, n):
    """Yield successive n-sized chunks from lst."""

    # from https://stackoverflow.com/a/312464/1144523
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch(ids):
    gh = GithubApiSession()

    all_data = []
    for ids_chunk in _chunks(ids, 100):

        crnt_data = gh.query(
            query,
            node_ids = ids_chunk
        )

        all_data.append(crnt_data["data"])

    return all_data


def clean(data):
    q = """
    .[]| .nodes[] | {
      id: .id,
      email: .email,
      bio: .bio,
      created_at: .createdAt,
      login: .login,
      updated_at: .updatedAt,
      twitter_username: .twitterUsername,
    }
    """
    return jq.compile(q).input(data).all()
