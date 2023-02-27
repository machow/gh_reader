import jq
from gh_reader.gh_api import GithubApiSession, GithubApiError, GithubApiRateLimitError

query_issue_events = """
fragment actorId on Actor {
  ... on User {
    id
  }
  ... on Bot {
    id
  }
  ... on Organization {
    id
  }
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
    ... on User {
      user_id: id
    }
    ... on Bot {
      user_id: id
    }
    ... on Organization {
      user_id: id
    }
  }
  body
  created_at: createdAt
  updated_at: updatedAt
}

fragment issueTimelineItemsData on IssueTimelineItems {
  ... on AddedToProjectEvent {
    actor {
      ...actorId
    }
    createdAt
    databaseId
    id
  }

  ... on AssignedEvent {
    actor {
      ...actorId
    }
    assignable {
      ...assignableId
    }
    assignee {
      ... on User {
        id
      }
      ... on Bot {
        id
      }
    }
    createdAt
    id
    user {
      id
    }
  }

  ... on ClosedEvent {
    actor {
      ...actorId
    }
    closable {
      closed
      closedAt

      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
    closer {
      ... on Commit {
        id
        type: __typename
      }
      ... on PullRequest {
        id
        type: __typename
      }
    }
    createdAt
    id
    resourcePath
    stateReason
    url
  }

  ... on CommentDeletedEvent {
    actor {
      ...actorId
    }
    createdAt
    databaseId
    deletedCommentAuthor {
      ...actorId
    }
    id
  }

  ... on ConnectedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    isCrossRepository
    source {
      ...referencedSubjectId
    }
    subject {
      ...referencedSubjectId
    }
  }

  ... on ConvertedNoteToIssueEvent {
    actor {
      ...actorId
    }
    createdAt
    databaseId
    id
  }

  ... on ConvertedToDiscussionEvent {
    actor {
      ...actorId
    }
    createdAt
    discussion {
      id
    }
    id
  }

  ... on CrossReferencedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    isCrossRepository
    referencedAt
    resourcePath
    source {
      ...referencedSubjectId
    }
    target {
      ...referencedSubjectId
    }
    url
    willCloseTarget
  }

  ... on DemilestonedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    milestoneTitle
    subject {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
  }

  ... on DisconnectedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    isCrossRepository
    source {
      ...referencedSubjectId
    }
    subject {
      ...referencedSubjectId
    }
  }

  ...eventIssueCommentData

  ... on LabeledEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    label {
      id
    }
    labelable {
      ...labelableId
    }
  }

  ... on LockedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    lockReason
    lockable {
      ...lockableId
    }
  }

  ... on MarkedAsDuplicateEvent {
    actor {
      ...actorId
    }
    canonical {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
    createdAt
    duplicate {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
    id
    isCrossRepository
  }

  ... on MentionedEvent {
    actor {
      ...actorId
    }
    createdAt
    databaseId
    id
  }

  ... on MilestonedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    milestoneTitle
    subject {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
  }

  ... on MovedColumnsInProjectEvent {
    actor {
      ...actorId
    }
    createdAt
    databaseId
    id
  }

  ... on PinnedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    issue {
      id
    }
  }

  ... on ReferencedEvent {
    actor {
      ...actorId
    }
    commit {
      id
    }
    commitRepository {
      id
    }
    createdAt
    id
    isCrossRepository
    isDirectReference
    subject {
      ...referencedSubjectId
    }
  }

  ... on RemovedFromProjectEvent {
    actor {
      ...actorId
    }
    createdAt
    databaseId
    id
  }

  ... on RenamedTitleEvent {
    actor {
      ...actorId
    }
    createdAt
    currentTitle
    id
    previousTitle
    subject {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
  }

  ... on ReopenedEvent {
    actor {
      ...actorId
    }
    closable {
      closed
      closedAt

      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
    createdAt
    id
    stateReason
  }

  ... on SubscribedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    subscribable {
      viewerCanSubscribe
      viewerSubscription
    }
  }

  ... on TransferredEvent {
    actor {
      ...actorId
    }
    createdAt
    fromRepository {
      id
    }
    id
    issue {
      id
    }
  }

  ... on UnassignedEvent {
    actor {
      ...actorId
    }
    assignable {
      ...assignableId
    }
    assignee {
      ... on User {
        id
      }
      ... on Bot {
        id
      }
    }
    createdAt
    id
    user {
      id
    }
  }

  ... on UnlabeledEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    label {
      id
    }
    labelable {
      ...labelableId
    }
  }

  ... on UnlockedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    lockable {
      ...lockableId
    }
  }

  ... on UnmarkedAsDuplicateEvent {
    actor {
      ...actorId
    }
    canonical {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
    createdAt
    duplicate {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
    id
    isCrossRepository
  }

  ... on UnpinnedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    issue {
      id
    }
  }

  ... on UnsubscribedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    subscribable {
      viewerCanSubscribe
      viewerSubscription
    }
  }

  ... on UserBlockedEvent {
    actor {
      ...actorId
    }
    blockDuration
    createdAt
    id
    subject {
      id
    }
  }
}
fragment pullRequestTimelineItemsData on PullRequestTimelineItems {
  ... on AddedToProjectEvent {
    actor {
      ...actorId
    }
    createdAt
    databaseId
    id
  }

  ... on AssignedEvent {
    actor {
      ...actorId
    }
    assignable {
      ...assignableId
    }
    assignee {
      ... on User {
        id
      }
      ... on Bot {
        id
      }
    }
    createdAt
    id
    user {
      id
    }
  }

  ... on ClosedEvent {
    actor {
      ...actorId
    }
    closable {
      closed
      closedAt

      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
    closer {
      ... on Commit {
        id
        type: __typename
      }
      ... on PullRequest {
        id
        type: __typename
      }
    }
    createdAt
    id
    resourcePath
    stateReason
    url
  }

  ... on CommentDeletedEvent {
    actor {
      ...actorId
    }
    createdAt
    databaseId
    deletedCommentAuthor {
      ...actorId
    }
    id
  }

  ... on ConnectedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    isCrossRepository
    source {
      ...referencedSubjectId
    }
    subject {
      ...referencedSubjectId
    }
  }

  ... on ConvertedNoteToIssueEvent {
    actor {
      ...actorId
    }
    createdAt
    databaseId
    id
  }

  ... on ConvertedToDiscussionEvent {
    actor {
      ...actorId
    }
    createdAt
    discussion {
      id
    }
    id
  }

  ... on CrossReferencedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    isCrossRepository
    referencedAt
    resourcePath
    source {
      ...referencedSubjectId
    }
    target {
      ...referencedSubjectId
    }
    url
    willCloseTarget
  }

  ... on DemilestonedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    milestoneTitle
    subject {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
  }

  ... on DisconnectedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    isCrossRepository
    source {
      ...referencedSubjectId
    }
    subject {
      ...referencedSubjectId
    }
  }

  ...eventIssueCommentData

  ... on LabeledEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    label {
      id
    }
    labelable {
      ...labelableId
    }
  }

  ... on LockedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    lockReason
    lockable {
      ...lockableId
    }
  }

  ... on MarkedAsDuplicateEvent {
    actor {
      ...actorId
    }
    canonical {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
    createdAt
    duplicate {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
    id
    isCrossRepository
  }

  ... on MentionedEvent {
    actor {
      ...actorId
    }
    createdAt
    databaseId
    id
  }

  ... on MilestonedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    milestoneTitle
    subject {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
  }

  ... on MovedColumnsInProjectEvent {
    actor {
      ...actorId
    }
    createdAt
    databaseId
    id
  }

  ... on PinnedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    issue {
      id
    }
  }

  ... on ReferencedEvent {
    actor {
      ...actorId
    }
    commit {
      id
    }
    commitRepository {
      id
    }
    createdAt
    id
    isCrossRepository
    isDirectReference
    subject {
      ...referencedSubjectId
    }
  }

  ... on RemovedFromProjectEvent {
    actor {
      ...actorId
    }
    createdAt
    databaseId
    id
  }

  ... on RenamedTitleEvent {
    actor {
      ...actorId
    }
    createdAt
    currentTitle
    id
    previousTitle
    subject {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
  }

  ... on ReopenedEvent {
    actor {
      ...actorId
    }
    closable {
      closed
      closedAt

      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
    createdAt
    id
    stateReason
  }

  ... on SubscribedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    subscribable {
      viewerCanSubscribe
      viewerSubscription
    }
  }

  ... on TransferredEvent {
    actor {
      ...actorId
    }
    createdAt
    fromRepository {
      id
    }
    id
    issue {
      id
    }
  }

  ... on UnassignedEvent {
    actor {
      ...actorId
    }
    assignable {
      ...assignableId
    }
    assignee {
      ... on User {
        id
      }
      ... on Bot {
        id
      }
    }
    createdAt
    id
    user {
      id
    }
  }

  ... on UnlabeledEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    label {
      id
    }
    labelable {
      ...labelableId
    }
  }

  ... on UnlockedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    lockable {
      ...lockableId
    }
  }

  ... on UnmarkedAsDuplicateEvent {
    actor {
      ...actorId
    }
    canonical {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
    createdAt
    duplicate {
      ... on Issue {
        id
      }
      ... on PullRequest {
        id
      }
    }
    id
    isCrossRepository
  }

  ... on UnpinnedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    issue {
      id
    }
  }

  ... on UnsubscribedEvent {
    actor {
      ...actorId
    }
    createdAt
    id
    subscribable {
      viewerCanSubscribe
      viewerSubscription
    }
  }

  ... on UserBlockedEvent {
    actor {
      ...actorId
    }
    blockDuration
    createdAt
    id
    subject {
      id
    }
  }
}
query issueEvents($node_ids: [ID!]!) {
  nodes(ids: $node_ids) {
    ... on Issue {
      id
      timelineItems(first: 100) {
        nodes {
          type: __typename
          ...issueTimelineItemsData
        }
      }
    }
    ... on PullRequest {
      id
      timelineItems(first: 100) {
        nodes {
          type: __typename
          ...pullRequestTimelineItemsData
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


def retry(f, n=1):
    import time

    n_tries = 0

    while n_tries <= n:
        try:
            return f()
        except GithubApiRateLimitError as e:
            raise e
        except Exception as e:
            err = e
            print(f"job failed: {e}")
            print(f"retry number {n_tries + 1}")
            n_tries += 1
            time.sleep(5)

    raise err

def fetch(ids, api_key=None):
    gh = GithubApiSession(api_key)

    all_data = []
    for ids_chunk in _chunks(ids, 40):

        try:
            crnt_data = retry(
                lambda: gh.query(query_issue_events, node_ids = ids_chunk),
                n = 2
            )
            all_data.append(crnt_data["data"])
        except GithubApiError as e:
            # try to query fewer node ids
            # for some reason, repos like tidyverse/dplyr have sequences of
            # ids that error when you query 10 or more. seems like a rare case.
            print("ISSUE_EVENTS: attempting to download smaller chunks...")
            for sub_chunk in _chunks(ids_chunk, 2):
                crnt_data = retry(
                    lambda: gh.query(query_issue_events, node_ids = sub_chunk),
                    n = 1
                )
                all_data.append(crnt_data["data"])



    return all_data


def clean(data):
    q = ".[] | .nodes[] | . as $issue | .timelineItems.nodes[] | {issue_id: $issue.id} + ."
    return jq.compile(q).input(data).all()
