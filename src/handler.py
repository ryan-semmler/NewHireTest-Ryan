import json
import os
import csv
import datetime
import re
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
    email_pattern = r'\s*[\w_\-.%]+@(\w+-?)*\w+\.[a-zA-Z]{2,}\s*$'
    event_user_ids = []
    for user in event_data:
        # standardize data
        user_data = {}
        for key, val in user.items():
            key = new_keys[key] if key in new_keys else key.lower()
            if key == 'salary':
                try:
                    val = int(val)
                except ValueError as e:
                    response_body['errors'].append(str(e))
                    continue
            elif key == 'hire_date':
                val = datetime.datetime.strptime(val, '%m/%d/%Y')
            elif key == 'manager_id' and val:
                manager = db.user.find_one({'normalized_email': user['Manager']})
                if manager:
                    val = manager['_id']
                # else value will remain an email address for now
            elif key == 'normalized_email':
                # check that email is valid
                if not re.match(email_pattern, val):
                    response_body['errors'].append(f"{val} is not a valid email address.")
                # normalize email
                val = val.lower().strip()
            user_data[key] = val

        # check whether user already exists in db
        existing_user = db.user.find_one({'normalized_email': user_data['normalized_email']})
        if existing_user:
            # update existing user
            db.user.update_one({'normalized_email': user_data['normalized_email']},
                               {"$set": user_data})
            response_body['numUpdated'] += 1

            # update chain of command
            managers = []
            if user_data['manager_id']:
                managers.append(user_data['manager_id'])
                db.chain_of_command.update_one({'user_id': existing_user['_id']},
                                               {"$set": {'chain_of_command': managers}})

            event_user_ids.append(existing_user['_id'])
        else:
            # insert user
            db.user.insert_one(user_data)
            response_body['numCreated'] += 1

            # insert chain of command
            managers = []
            if user_data['manager_id']:
                managers.append(user_data['manager_id'])
            db.chain_of_command.insert_one({"user_id": user_data['_id'],
                                            "chain_of_command": managers})

            event_user_ids.append(user_data['_id'])

    # update manager_id for users with an email address as manager_id
    for user in db.user.find({'manager_id': {'$regex': email_pattern}}):
        # update chain of command to use the manager's _id
        manager_id = db.user.find_one({'normalized_email': user['manager_id']})['_id']
        db.chain_of_command.update_one({'user_id': user['_id']},
                                       {"$set": {"chain_of_command": [manager_id]}})
        # update user to use manager's _id as manager_id
        db.user.update_one({'normalized_email': user['normalized_email']},
                           {"$set": {'manager_id': manager_id}})

    def update_chain_of_command(user_id):
        """
        Updates chains of command to include more than the user's immediate manager.
        Then updates each of the user's subordinates' chains of command to include the change.
        """
        chain_of_command = []
        if isinstance(user_id, dict):
            _id = user_id['_id']
        else:
            _id = user_id
        user = db.user.find_one({'_id': _id})
        current_user = user
        while current_user['manager_id']:
            manager_id = current_user['manager_id']
            chain_of_command.append(manager_id)
            current_user = db.user.find_one({'_id': manager_id})
        db.chain_of_command.update_one({'user_id': user['_id']},
                                       {"$set": {'chain_of_command': chain_of_command}})
        # also update the user's subordinates' chains of command
        for subordinate in db.user.find({'manager_id': user['_id']}):
            update_chain_of_command(subordinate)

    for user_id in event_user_ids:
        update_chain_of_command(user_id)

    response = {
        "statusCode": 200,
        "body": json.dumps(response_body)
    }

    return response
