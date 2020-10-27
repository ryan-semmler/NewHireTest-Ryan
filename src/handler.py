import json
import os
import csv
import datetime
from pymongo import MongoClient

db_uri = os.environ.get("MONGO_DB_URI", "localhost")
db_name = os.environ.get("MONGO_DB_NAME", "new_hire_test")

db = MongoClient(db_uri)[db_name]


def handle_csv_upload(event, context):
    response_body = {
        "numCreated": 0,
        "numUpdated": 0,
        "errors": [],
    }

    # get users from event
    event_data = csv.DictReader(event.splitlines())

    new_keys = {
        'Email': 'normalized_email',
        'Manager': 'manager_id',
        'Hire Date': 'hire_date',
    }

    for user in event_data:
        # standardize data
        user_data = {}
        for k, v in user.items():
            k = new_keys[k] if k in new_keys else k.lower()
            if k == 'salary':
                v = int(v)
            elif k == 'hire_date':
                v = datetime.datetime.strptime(v, '%m/%d/%Y')
            user_data[k] = v



        # user_data = {new_keys[k] if k in new_keys else k.lower(): v for k, v in user.items()}

        # check whether user already exists in db
        existing_user = db.user.find_one({'normalized_email': user_data['normalized_email']})

        if existing_user:
            # update existing user
            db.user.update_one({'normalized_email': user_data['normalized_email']},
                               {"$set": user_data})
            response_body['numUpdated'] += 1
        else:
            # create new user
            db.user.insert_one(user_data)
            response_body['numCreated'] += 1

        # # update field names
        # translation = {'Email': 'normalized_email'}
        # update = {"$rename": translation}
        # db.user.update({'Email': user['Email']}, update)

        import pdb; pdb.set_trace()

    response = {
        "statusCode": 200,
        "body": json.dumps(response_body)
    }

    return response
