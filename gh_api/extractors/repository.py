import jq

from gh_api.gh_api import GithubApiSession

query = """
   query stargazers($owner: String!, $name: String!) {
     repository(name: $name, owner: $owner) {
       id
       isArchived
       owner {
         ... on Organization {
           id
         }

         ... on User {
           id
         }
       }
       createdAt
       defaultBranchRef {
         name
       }
       description
       isFork
       nameWithOwner
       name
       homepageUrl
       isPrivate
     }
   }
"""


def fetch(owner="tidyverse", name="dplyr", api_key=None):
    gh = GithubApiSession(api_key)

    return gh.query(
        query,
        owner=owner,
        name=name
    )["data"]


def clean(data):
    q = """
      .repository | {
        id: .id,
        is_archived: .isArchived,
        owner_id: .owner.id,
        created_at: .createdAt,
        default_branch: .defaultBranchRef.name,
        description: .description,
        is_fork: .isFork,
        full_name: .nameWithOwner,
        homepage: .homepageUrl,
        # language: ,
        name: .name,
        is_private: .isPrivate 
      }
    """
    return jq.compile(q).input(data).all()

