<p align="center">
  <kbd><img src="https://raw.githubusercontent.com/AppSolves/fastapi-users-db-dynamodb/refs/heads/main/assets/github/repo_card.png?sanitize=true" alt="FastAPI Users DynamoDB Adapter"></kbd>
</p>
<br>

# FastAPI Users - Database adapter for AWS DynamoDB

<p align="center">
  <img src="https://raw.githubusercontent.com/frankie567/fastapi-users/master/logo.svg?sanitize=true" alt="FastAPI Users">
</p>

<p align="center">
    <em>Ready-to-use and customizable users management for FastAPI</em>
</p>

[![build](https://github.com/AppSolves/fastapi-users-db-dynamodb/workflows/Build/badge.svg)](https://github.com/fastapi-users/fastapi-users/actions)
[![PyPI version](https://badge.fury.io/py/fastapi-users-db-dynamodb.svg)](https://badge.fury.io/py/fastapi-users-db-dynamodb)
[![Downloads](https://pepy.tech/badge/fastapi-users-db-dynamodb)](https://pepy.tech/project/fastapi-users-db-dynamodb)

---

**Documentation**: <a href="https://fastapi-users.github.io/fastapi-users/" target="_blank">https://fastapi-users.github.io/fastapi-users/</a>

**Source Code**: <a href="https://github.com/fastapi-users/fastapi-users" target="_blank">https://github.com/fastapi-users/fastapi-users</a>

---

Add quickly a registration and authentication system to your [FastAPI](https://fastapi.tiangolo.com/) project. **FastAPI Users** is designed to be as customizable and adaptable as possible.

**Sub-package for AWS DynamoDB support in FastAPI Users.**

## Development

### Setup environment

We use [Hatch](https://hatch.pypa.io/latest/install/) to manage the development environment and production build. Ensure it's installed on your system.

### Run unit tests

You can run all the tests with:

```bash
hatch run test
```

### Format the code

Execute the following command to apply `isort` and `black` formatting:

```bash
hatch run lint
```

## License

This project is licensed under the terms of the Apache 2.0 license.
See the LICENSE and NOTICE files for more information.