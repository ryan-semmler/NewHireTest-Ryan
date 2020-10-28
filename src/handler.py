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
    event_user_ids = []

    for user in event_data:
        # standardize data
        user_data = {}
        for k, v in user.items():
            k = new_keys[k] if k in new_keys else k.lower()
            if k == 'salary':
                try:
                    v = int(v)
                except ValueError as e:
                    response_body['errors'].append(str(e))
                    continue
            elif k == 'hire_date':
                v = datetime.datetime.strptime(v, '%m/%d/%Y')
            elif k == 'manager_id' and v:
                manager = db.user.find_one({'normalized_email': user['Manager']})
                if manager:
                    v = manager['_id']
                # otherwise value will remain an email address for now
            elif k == 'normalized_email':
                if not re.match(r'\s*[\w_\-.%]+@(\w+-?)*\w+\.[a-zA-Z]{2,}\s*$', v):  # check that email addr is valid
                    response_body['errors'].append(f"{v} is not a valid email address.")
                v = v.lower().strip()  # normalize
            user_data[k] = v


        # check whether user already exists in db
        existing_user = db.user.find_one({'normalized_email': user_data['normalized_email']})
        # import pdb; pdb.set_trace()

        if existing_user:
            # # insert chain of command
            # user_record = db.user.find_one({'normalized_email': user_data['normalized_email']})
            # manager_id = user_data['manager_id']
            # user_id = user_record['_id']
            #
            # data = {"user_id": user_id,
            #         "chain_of_command": []}
            # # import pdb; pdb.set_trace()
            # if manager_id:
            #     data['chain_of_command'].append(manager_id)
            #     current_user = db.user.find_one({'_id': manager_id})
            #     while current_user['manager_id']:
            #         data['chain_of_command'].append(user_data['manager_id'])
            #         current_user = db.user.find_one({'_id': user_data['manager_id']})
            # update existing user
            db.user.update_one({'normalized_email': user_data['normalized_email']},
                               {"$set": user_data})
            response_body['numUpdated'] += 1
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

            # # insert chain of command
            # data = {"user_id": user_data['_id'],
            #         "chain_of_command": []}
            # current_user = db.user.find_one({'_id': user_data['_id']})
            # while current_user['manager_id']:
            #     data['chain_of_command'].append(current_user['manager_id'])
            #     current_user = db.user.find_one({'_id': current_user['manager_id']})
            #
            # db.chain_of_command.insert_one(data)
            managers = []
            if user_data['manager_id']:
                managers.append(user_data['manager_id'])
            db.chain_of_command.insert_one({"user_id": user_data['_id'],
                                            "chain_of_command": managers})
            event_user_ids.append(user_data['_id'])


    # import pdb; pdb.set_trace()

    # iterate users again to convert manager_ids from emails to ObjectIDs
    # TODO store pattern as variable

    # update manager_id for users with an email address as manager_id
    extend_chain_users = []
    for user in db.user.find({'manager_id': {'$regex': r'\s*[\w_\-.%]+@(\w+-?)*\w+\.[a-zA-Z]{2,}\s*$'}}):
        # insert chain of command
        manager_id = db.user.find_one({'normalized_email': user['manager_id']})['_id']
        db.chain_of_command.update_one({'user_id': user['_id']},
                                       {"$set": {"chain_of_command": [manager_id]}})
        db.user.update_one({'normalized_email': user['normalized_email']},
                           {"$set": {'manager_id': manager_id}})
        extend_chain_users.append(user)

    def update_chain_of_command(user_id):
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

        #
        # current_user = user
        # while current_user['manager_id']:
        #     manager_id = current_user['manager_id']
        #     if isinstance(current_user['manager_id'], str):
        #         manager_id = db.user.find_one({'normalized_email': manager_id})
        #     data['chain_of_command'].append(manager_id)
        #     current_user = db.user.find_one({'_id': manager_id})
        # db.chain_of_command.update_one({'user_id': user['_id']},
        #                                {"$set": data})

        #
        # user_record = db.user.find_one({'normalized_email': user_data['normalized_email']})
        # manager_id = user_data['manager_id']
        # user_id = user_record['_id']
        # data = {"user_id": user_id,
        #         "chain_of_command": []}
        # if manager_id:
        #     data['chain_of_command'].append(manager_id)
        #     current_user = db.user.find_one({'_id': manager_id})
        #     while current_user['manager_id']:
        #         data['chain_of_command'].append(user_data['manager_id'])
        #         current_user = db.user.find_one({'_id': user_data['manager_id']})
        # db.chain_of_command.update_one({'user_id': user_id},
        #                                {"$set": data})

    response = {
        "statusCode": 200,
        "body": json.dumps(response_body)
    }

    return response
