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
            elif k == 'manager_id' and v:
                v = db.user.find_one({'normalized_email': user['Manager']})['_id']
            user_data[k] = v

        # check whether user already exists in db
        existing_user = db.user.find_one({'normalized_email': user_data['normalized_email']})
        if existing_user:
            # update existing user
            db.user.update_one({'normalized_email': user_data['normalized_email']},
                               {"$set": user_data})
            response_body['numUpdated'] += 1
        else:
            # insert user
            db.user.insert_one(user_data)
            response_body['numCreated'] += 1

            # insert chain of command
            data = {"user_id": user_data['_id'],
                    "chain_of_command": []}
            current_user = db.user.find_one({'_id': user_data['_id']})
            while current_user['manager_id']:
                data['chain_of_command'].append(current_user['manager_id'])
                current_user = db.user.find_one({'_id': current_user['manager_id']})
            db.chain_of_command.insert_one(data)

    response = {
        "statusCode": 200,
        "body": json.dumps(response_body)
    }

    return response
