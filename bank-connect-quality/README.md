# Bank Connect Quality

Steps to setup:
- Setup a virtual environment and then install dependencies in requirements.txt
    `pip install -r requirements.txt`
- Now execute the `setup.py` file, to create sqlite3 database
    `python setup.py`
- Now run the server
    `uvicorn main:app --workers 2`

> In case you are running locally, use `uvicorn main:app --reload`

