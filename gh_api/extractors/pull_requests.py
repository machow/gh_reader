import jq

from gh_api.gh_api import GithubApiSession

# TODO: missing fields like base_sha -- where are these found?

query_prs = """
    fragment prData on PullRequest {
      id
      baseRepository {
        id
        owner { id }
      }
      headRepository {
        id
        owner { id }
      }
      baseRefName
      isDraft
      headRefName
    }

    query prData($node_ids: [ID!]!) {
      nodes(ids: $node_ids) {
        ... prData
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
            query_prs,
            node_ids = ids_chunk
        )

        all_data.append(crnt_data["data"])

    return all_data


def clean(data):
    q = """
        .[]
        | .nodes[]
        | {
          id: .id,
          # base_sha: ,
          base_repo_id: .baseRepository.id,
          base_user_id: .baseRepository.owner.id,
          # head_sha: ,
          head_repo_id: .headRepository.id,
          head_user_id: .headRepository.owner.id,
          issue_id: .id,
          # merge_commit_sha: ,
          # base_label: ,
          base_ref: .baseRefName,
          draft: .isDraft,
          # head_label: ,
          head_ref: .headRefName
        }
      
    """
    return jq.compile(q).input(data).all()
