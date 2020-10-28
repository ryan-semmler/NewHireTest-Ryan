from handler import db, handle_csv_upload
import json
import datetime
import pymongo
import bcrypt
from bson import ObjectId


def dummy_data_decorator(test_function):
    def f():
        """
        Drop any existing data and fill in some dummy test data,
        as well as creating indexes; the data will be dropped after
        the test as well
        """

        db.user.drop()
        db.user.create_index([
            ("normalized_email", pymongo.ASCENDING),
        ], unique=True)

        dummy_users = [
            {
                "_id": ObjectId(),
                "name": "Brad Jones",
                "normalized_email": "bjones@performyard.com",
                "manager_id": None,
                "salary": 90000,
                "hire_date": datetime.datetime(2010, 2, 10),
                "is_active": True,
                "hashed_password": bcrypt.hashpw(b"password", bcrypt.gensalt()),
            },
            {
                "_id": ObjectId(),
                "name": "Ted Harrison",
                "normalized_email": "tharrison@performyard.com",
                "manager_id": None,
                "salary": 50000,
                "hire_date": datetime.datetime(2012, 10, 20),
                "is_active": True,
                "hashed_password": bcrypt.hashpw(b"correct horse battery staple", bcrypt.gensalt()),
            }
        ]

        # Give Ted a manager
        dummy_users[1]["manager_id"] = dummy_users[0]["_id"]

        for user in dummy_users:
            db.user.insert(user)

        db.chain_of_command.drop()
        db.chain_of_command.create_index([
            ("user_id", pymongo.ASCENDING),
        ], unique=True)

        dummy_chain_of_commands = [
            {"user_id": dummy_users[0]["_id"], "chain_of_command":[]},
            # edited this so that only brad's ID shows up in the chain of command
            {"user_id": dummy_users[1]["_id"], "chain_of_command":[dummy_users[0]["_id"]]},
        ]

        for chain_of_command in dummy_chain_of_commands:
            db.chain_of_command.insert(chain_of_command)

        test_function()
        db.user.drop()
        db.chain_of_command.drop()
    return f


@dummy_data_decorator
def test_setup():
    """
    This test should always pass if your environment is set up correctly
    """
    assert True


@dummy_data_decorator
def test_simple_csv():
    """
    This should successfully update one user and create one user,
    also updating their chain of commands appropriately
    """

    body = """Name,Email,Manager,Salary,Hire Date
Brad Jones,bjones@performyard.com,,100000,02/10/2010
John Smith,jsmith@performyard.com,bjones@performyard.com,80000,07/16/2018
"""

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 0)

    # Check that we added the correct number of users
    assert(db.user.count() == 3)
    assert(db.chain_of_command.count() == 3)

    # Check that Brad's salary was updated
    brad = db.user.find_one({"normalized_email": "bjones@performyard.com"})
    assert(brad["salary"] == 100000)

    # Check that Brad's chain of command is still empty
    brad_chain_of_command = db.chain_of_command.find_one(
        {"user_id": brad["_id"]})
    assert(len(brad_chain_of_command["chain_of_command"]) == 0)

    # Check that John's data was inserted correctly
    john = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(john["name"] == "John Smith")
    assert(john["salary"] == 80000)
    assert(john["manager_id"] == brad["_id"])
    assert(john["hire_date"] == datetime.datetime(2018, 7, 16))

    # Check that Brad is in John's chain of command
    john_chain_of_command = db.chain_of_command.find_one(
        {"user_id": john["_id"]})
    assert(len(john_chain_of_command["chain_of_command"]) == 1)
    assert(john_chain_of_command["chain_of_command"][0] == brad["_id"])


@dummy_data_decorator
def test_invalid_number():
    """
    This test should still update Brad and create John, but should return
    a single error because the salary field for Brad isn't a number
    """

    body = """Name,Email,Manager,Salary,Hire Date
Bradley Jones,bjones@performyard.com,,NOT A NUMBER,02/10/2010
John Smith,jsmith@performyard.com,bjones@performyard.com,80000,07/16/2018
"""

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 1)

    # Check that we added the correct number of users
    assert(db.user.count() == 3)
    assert(db.chain_of_command.count() == 3)

    # Check that Brad's salary was updated
    brad = db.user.find_one({"normalized_email": "bjones@performyard.com"})
    assert(brad["salary"] == 90000)
    assert(brad["name"] == "Bradley Jones")

    # Check that Brad's chain of command is still empty
    brad_chain_of_command = db.chain_of_command.find_one(
        {"user_id": brad["_id"]})
    assert(len(brad_chain_of_command["chain_of_command"]) == 0)

    # Check that John's data was inserted correctly
    john = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(john["name"] == "John Smith")
    assert(john["salary"] == 80000)
    assert(john["manager_id"] == brad["_id"])
    assert(john["hire_date"] == datetime.datetime(2018, 7, 16))

    # Check that Brad is in John's chain of command
    john_chain_of_command = db.chain_of_command.find_one(
        {"user_id": john["_id"]})
    assert(len(john_chain_of_command["chain_of_command"]) == 1)
    assert(john_chain_of_command["chain_of_command"][0] == brad["_id"])


@dummy_data_decorator
def test_invalid_email():
    """
    This test adds nine new users with different invalid emails.
    Each user should still be created, but an error should be logged for each invalid email.
    """

    # users with invalid email addresses
    body = """Name,Email,Manager,Salary,Hire Date
John1,jsmith.com,,80000,07/16/2018
John2,jsmith@performyard,,80000,07/16/2018
John3,jsmith(1)@performyard.com,,80000,07/16/2018
John4,jsmith@performyard@py.com,,80000,07/16/2018
John5,jsmith@perform--yard.com,,80000,07/16/2018
John6,jsmith@-performyard.com,,80000,07/16/2018
John7,jsmith@performyard-.com,,80000,07/16/2018
John8,jsmith@performyard.c,,80000,07/16/2018
John9,jsmith@performyard.co2,,80000,07/16/2018
"""

    response = handle_csv_upload(body, {})
    assert (response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert (body["numCreated"] == 9)
    assert (body["numUpdated"] == 0)
    assert (len(body["errors"]) == 9)

    # Check that we added the correct number of users
    assert (db.user.count() == 11)
    assert (db.chain_of_command.count() == 11)


@dummy_data_decorator
def test_valid_email():
    """
    This test adds eight new users with different valid emails.
    Verify that different valid email formats get added successfully.
    """

    # users with valid email addresses
    body = """Name,Email,Manager,Salary,Hire Date
John1,jsmith@performyard.com,,80000,07/16/2018
John2,JSMITH@PY.NET,,80000,07/16/2018
John3,j_smith@peformyard.web,,80000,07/16/2018
John4,j.Smith@py.pizza,,80000,07/16/2018
John5,jsmith123@perf.org,,80000,07/16/2018
John6,j-smith@performyard.com,,80000,07/16/2018
John7,jsmith@perform-yard.com,,80000,07/16/2018
John8,jsmith@performyard2.com,,80000,07/16/2018
"""

    response = handle_csv_upload(body, {})
    assert (response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert (body["numCreated"] == 8)
    assert (body["numUpdated"] == 0)
    assert (len(body["errors"]) == 0)

    # Check that we added the correct number of users
    assert (db.user.count() == 10)
    assert (db.chain_of_command.count() == 10)


@dummy_data_decorator
def test_duplicate_name():
    """
    This test verifies that different users can be added to the db with the same name,
    as long as they have different emails.
    """

    body = """Name,Email,Manager,Salary,Hire Date
Bradley Jones,bjones@performyard.com,,90000,02/10/2010
Bradley Jones,bradj@performyard.com,bjones@performyard.com,90001,07/16/2018
"""

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 0)

    # Check that we added the correct number of users
    assert(db.user.count() == 3)
    assert(db.chain_of_command.count() == 3)

    # Check that there are two users with the name Bradley Jones
    assert(db.user.find({'name': 'Bradley Jones'}).count() == 2)


@dummy_data_decorator
def test_duplicate_email():
    """
    This test checks behavior when adding multiple records with the same email address.
    Users with the same email should be considered the same user, so duplicate users should be updated.
    """

    body = """Name,Email,Manager,Salary,Hire Date
Bradley Jones,bjones@performyard.com,,90000,02/10/2010
John Smith,bjones@performyard.com,,80000,07/16/2018
"""

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 0)
    assert(body["numUpdated"] == 2)
    assert(len(body["errors"]) == 0)

    # Check that we added the correct number of users
    assert(db.user.count() == 2)
    assert(db.chain_of_command.count() == 2)

    # Check that there's only one user with the duplicated email address
    assert(db.user.find({'normalized_email': 'bjones@performyard.com'}).count() == 1)


@dummy_data_decorator
def test_chain_of_command():
    """
    This test verifies that longer chains of command get handled correctly.
    """

    body = """Name,Email,Manager,Salary,Hire Date
Sara Bossman,sbossman@performyard.com,,110000,01/01/2020
Bradley Jones,bjones@performyard.com,sbossman@performyard.com,90000,02/10/2010
John Smith,jsmith@performyard.com,bjones@performyard.com,80000,07/16/2018
"""

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 2)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 0)

    # Check that we added the correct number of users
    assert(db.user.count() == 4)
    assert(db.chain_of_command.count() == 4)

    # Check that each user has the correct number of users in their chain of command
    for name, cc_len in (("Sara Bossman", 0), ("Bradley Jones", 1), ("John Smith", 2)):
        user = db.user.find_one({'name': name})
        chain = db.chain_of_command.find_one({'user_id': user['_id']})
        assert(len(chain['chain_of_command']) == cc_len)


@dummy_data_decorator
def test_manager_created_after_subordinate():
    """
    This test verifies that chain of command is still handled correctly when a manager user
    is created after a subordinate.
    """

    body = """Name,Email,Manager,Salary,Hire Date
Bradley Jones,bjones@performyard.com,sbossman@performyard.com,90000,02/10/2010
Sara Bossman,sbossman@performyard.com,,110000,01/01/2020
"""

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 0)

    # Check that we added the correct number of users
    assert(db.user.count() == 3)
    assert(db.chain_of_command.count() == 3)

    # Check that each user has the correct number of users in their chain of command
    for name, cc_len in (("Sara Bossman", 0), ("Bradley Jones", 1)):
        user = db.user.find_one({'name': name})
        chain = db.chain_of_command.find_one({'user_id': user['_id']})
        assert(len(chain['chain_of_command']) == cc_len)

    # Check that Brad's manager_id is Sara's user_id
    manager_id = db.user.find_one({'name': 'Bradley Jones'})['manager_id']
    assert(isinstance(manager_id, ObjectId))
    manager = db.user.find_one({'_id': manager_id})
    assert manager
    assert(manager['name'] == 'Sara Bossman')
