import json

events = {
    "AddedToProjectEvent",
    "AssignedEvent",
    "ClosedEvent",
    "CommentDeletedEvent",
    "ConnectedEvent",
    "ConvertedNoteToIssueEvent",
    "ConvertedToDiscussionEvent",
    "CrossReferencedEvent",
    "DemilestonedEvent",
    "DisconnectedEvent",
    "IssueComment",
    "LabeledEvent",
    "LockedEvent",
    "MarkedAsDuplicateEvent",
    "MentionedEvent",
    "MilestonedEvent",
    "MovedColumnsInProjectEvent",
    "PinnedEvent",
    "ReferencedEvent",
    "RemovedFromProjectEvent",
    "RenamedTitleEvent",
    "ReopenedEvent",
    "SubscribedEvent",
    "TransferredEvent",
    "UnassignedEvent",
    "UnlabeledEvent",
    "UnlockedEvent",
    "UnmarkedAsDuplicateEvent",
    "UnpinnedEvent",
    "UnsubscribedEvent",
    "UserBlockedEvent",
}

data = json.load(open("github_type_schema.json"))
all_types = data["data"]["__schema"]["types"]

event_schema = {entry["name"]: entry for entry in all_types if entry["name"] in events}

allowed_kinds = ["SCALAR", "ENUM"]

preamble = """
fragment actorId on Actor {
    ... on User { id }
    ... on Bot { id }
    ... on Organization { id }
}

fragment referencedSubjectId on ReferencedSubject {
   ... on Issue {
     id
   }
   ... on PullRequest {
     id
   }
}

fragment assignableId on Assignable {
   ... on Issue {
     id
   }
   ... on PullRequest {
     id
   }
}

fragment labelableId on Labelable {
   ... on Issue {
     type: __typename
     id
   }
   ... on PullRequest {
     type: __typename
     id
   }
   ... on Discussion {
     type: __typename
     id
   }
}

fragment lockableId on Lockable {
   ... on Issue {
     type: __typename
     id
   }
   ... on PullRequest {
     type: __typename
     id
   }
   ... on Discussion {
     type: __typename
     id
   }
}


fragment eventIssueCommentData on IssueComment {
    id
    issue {
    id
    number
    }
    author {
    ... on User { user_id: id }
    ... on Bot { user_id: id }
    ... on Organization { user_id: id }
    }
    body
    created_at: createdAt
    updated_at: updatedAt
}

"""

fragments = {
    "Assignable": """
       ... assignableId
    """,
    "Labelable": """
       ... labelableId
    """,
    "Lockable": """
       ... lockableId
    """,
    "ReactionConnection": None,
    "ReactionOrder": None,
    "UserContentEditConnection": None,


    "Actor": """
       ... actorId
    """,
    # TODO: add Organization { id } ?
    "Assignee": """
       ... on User { id }
       ... on Bot { id }
    """,
    "Discussion": """
       id
    """,
    "Issue": """
       id
    """,
    "User": """
       id
    """,
    "Closable": """
       closed
       closedAt

       ... on Issue {
         id
       }
       ... on PullRequest {
         id
       }
    """,
    "Closer": """
       ... on Commit {
           id
           type: __typename
       }
       ... on PullRequest {
           id
           type: __typename
       }
    """,
    "Commit": """
       id
    """,
    "IssueOrPullRequest": """
       ... on Issue {
         id
       }
       ... on PullRequest {
         id
       }
    """,
    "Label": """
       id
    """,
    "PullRequest": """
       id
    """,
    "ReactionGroup": """
       content
       createdAt
    """,
    "ReferencedSubject": """
       ... referencedSubjectId
    """,
    "RenamedTitleSubject": """
       ... on Issue {
         id
       }
       ... on PullRequest {
         id
       }
    """,
    "Repository": """
       id
    """,
    "Subscribable": """
       viewerCanSubscribe
       viewerSubscription
    """,
    "MilestoneItem": """
       ... on Issue {
         id
       }
       ... on PullRequest {
         id
       }
    """,
    "User": """
       id
    """,
}

overrides = {
    "IssueComment": """
       ... eventIssueCommentData
    """
}

def hash_dict(d):
    return hash(json.dumps(d))


def show_types(evt):
    return {field["name"]: field["type"] for field in evt["fields"]}


def get_final_type(type_):
    kind = type_["kind"]

    if kind in {"NON_NULL", "LIST"} and type_["ofType"] is not None:
        return get_final_type(type_["ofType"])

    return type_


def format_field(field):
    type_ = get_final_type(field["type"])
    kind = type_["kind"]
    type_name = type_["name"]

    
    if kind in allowed_kinds:
        return field["name"]

    fragment = fragments[type_name]

    if fragment is None:
        return ""
    else:
        return f"{field['name']} {{ {fragment} }}"


def get_unique_fields(schemas):
    hashes = set()
    all_fields = []
    for evt in schemas.values():
        for field in evt["fields"]:
            hashed = hash_dict(field)
            if hashed in hashes:
                continue
            else:
                hashes.add(hashed)
                all_fields.append(field)

    return all_fields


def generate_event_segment(event):
    if event["name"] in overrides:
        return overrides[event["name"]]

    field_entries = "\n".join(map(format_field, event["fields"]))
    event_name = event['name']
    return f"""
... on {event_name} {{
{field_entries}
}}
"""

def generate_query(events):
    event_parts = "\n\n".join(list(map(generate_event_segment, events.values())))

    issue_fragment = f"""fragment issueTimelineItemsData on IssueTimelineItems {{\n{event_parts}\n}}"""
    pr_fragment = f"""fragment pullRequestTimelineItemsData on PullRequestTimelineItems {{\n{event_parts}\n}}"""
    

    return f"""
    {preamble}
    {issue_fragment}
    {pr_fragment}
    query issueEvents($node_ids: [ID!]!) {{
      nodes(ids: $node_ids) {{
        ... on Issue {{
          timelineItems(first: 100) {{
            nodes {{
              type: __typename
              ... issueTimelineItemsData
            }}
          }}
        }}
        ... on PullRequest {{
          timelineItems(first: 100) {{
            type: __typename
            nodes {{
              ... pullRequestTimelineItemsData
            }}
          }}
        }}
      }}
    }}
    """

if __name__ == "__main__":
    print(generate_query(event_schema))
