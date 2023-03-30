# Example application code for the python architecture book

https://github.com/cosmicpython/book

https://github.com/cosmicpython/code

Examples have been refactored. Flask has been replaced with FastAPI. Asynchronous ORM has been used.

## Requirements

* docker with docker-compose
* for chapters 1 and 2, and optionally for the rest: a local python3.7 virtualenv

## Building the containers

_(this is only required from chapter 3 onwards)_

```sh
make build
make up
# or
make all # builds, brings containers up, runs tests
```

## Creating a local virtualenv (optional)

```sh
python3.7 -m venv .venv
poetry install
poetry shell # or source .venv/bin/activate
```

## Running the tests

```sh
make test
```

## Makefile

There are more useful commands in the makefile, have a look and try them out.
