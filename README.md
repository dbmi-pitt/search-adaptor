# Search Adaptor

The Search Adaptor is a thin wrapper of the Elasticsearch. It handles data indexing and reindexing into the backend Elasticsearch. It also accepts the search query and passes through to the Elasticsearch with data access security check. This adaptor can be used to create a RESTful web service interface into an Elasticsearch store where multiple indice pairs can be created, with each indice pair documents being transformed by separately provided transformer code. Each index in a pair is used for access level- ALL vs PUBLIC, where all documents are included in the ALL index and only PUBLICALLY available documents are added to the PUBLIC index. On a GET/search request the adaptor will check a user's credentials sending the request to the ALL or PUBLIC index depending on the user's access level.  Several layers of Translation can be provided to morph the data on the way to the indices.

Currently this search-adaptor is being used as a git submodule by the following projects:

- https://github.com/hubmapconsortium/search-api

## Development process

### To release via TEST infrastructure
- Make new feature or bug fix branches from `main` branch (the default branch)
- Make PRs to `main`
- As a codeowner, Zhou (github username `yuanzhou`) is automatically assigned as a reviewer to each PR. When all other reviewers have approved, he will approve as well, merge to TEST infrastructure, and redeploy and reindex the TEST instance.
- Developer or someone on the team who is familiar with the change will test/qa the change
- When any current changes in the `main` have been approved after test/qa on TEST, Zhou will release to PROD using the same docker image that has been tested on TEST infrastructure.

### To work on features in the development environment before ready for testing and releasing
- Make new feature branches off the `main` branch
- Make PRs to `dev-integrate`
- As a codeowner, Zhou is automatically assigned as a reviewer to each PR. When all other reviewers have approved, he will approve as well, merge to devel, and redeploy and reindex the DEV instance.
- When a feature branch is ready for testing and release, make a PR to `main` for deployment and testing on the TEST infrastructure as above.
