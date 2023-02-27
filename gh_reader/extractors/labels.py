import jq

from gh_reader.gh_api import GithubApiSession

query_label = """
    fragment labelData on Label {
      id
      color
      description
      is_default: isDefault
      name
      url
      repository { id }
    }

    query repoLabels($owner: String!, $name: String!) {
      repository(owner: $owner, name: $name) {
        labels(first: 100) {
          nodes {
            ... labelData
          }
        }
      }
    }
"""

def fetch(owner, name, api_key=None):
    gh = GithubApiSession(api_key)

    return gh.query(query_label, owner=owner, name=name)["data"]
    

def clean(data):
    q = """
        .repository.labels.nodes[] | del(.repository) + { repository_id: .repository.id }
    """
    return jq.compile(q).input(data).all()
