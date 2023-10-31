## Extractors

| name | arguments | notes | incremental |
| ---- | --------- | ----- | ----------- |
| commits | owner, name | | supported |
| issues | owner, name | | supported |
| issues_pr | owner, name | same schema as issues | supported |
| labels | owner, name | | |
| pull_requests | ids | | supported |
| repository | owner, name | | |
| stargazers | owner, name | | |
| users | ids | | |
| issue_comments | ids | Also included in issue_events records | supported^† |
| issue_events | ids | | supported^† |
| issue_labels | ids | | supported  | 

> †: Updates to these records do not affect the updatedAt field of their parent
  issue. Because issue updates are used to pull event updates, edits to records
  like IssueComment events may not get picked up in incremental updates.
