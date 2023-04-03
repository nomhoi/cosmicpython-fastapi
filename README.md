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

## Running a server locally

_(since chapter 06)_

```sh
poetry shell
cd <chapter folder>
export PYTHONPATH=$(pwd)/src:$(pwd)/tests
make postgres
uvicorn allocation.entrypoints.main:app --reload
pytest
```

## Debugging

_(since chapter 06)_

VS Code launch.json:
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: FastAPI",
            "type": "python",
            "request": "launch",
            "module": "uvicorn",
            "args": [
                "allocation.entrypoints.main:app",
                "--reload"
            ],
            "jinja": true,
            "justMyCode": true,
            "env": {
                "PYTHONPATH": "${cwd}/chapter_06_uow/src" // change to current chapter
            }
        }
    ]
}
```

Run a web-server locally.

https://fastapi.tiangolo.com/tutorial/debugging/
