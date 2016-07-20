"""
Removes passwords from config files

"""


import unittest
import json
import re
from urlparse import urlparse, urlunparse
import traceback

def is_valid_json(invalid_json):
    """
    Is the given string valid JSON?
    """
    got_exception = True
    try:
        json.loads(invalid_json)
        got_exception = False
    except Exception as e:
        pass

    return got_exception == False

def remove_passwords_from_db_config(database):
    """
    Given a dictionary with database level config, remove all passwords and sensitive info
    """
    if "server" in database:
        database["server"] = strip_password_from_url(database["server"])
    if "password" in database:
        database["password"] = "******"

    # bucket shadowing
    if "shadow" in database:
        shadow_settings = database["shadow"]
        if "server" in shadow_settings:
            shadow_settings["server"] = strip_password_from_url(shadow_settings["server"])
        if "password" in shadow_settings:
            shadow_settings["password"] = "******"

    # distributed index / sg accell
    if "channel_index" in database:
        channel_index_settings = database["channel_index"]
        if "server" in channel_index_settings:
            channel_index_settings["server"] = strip_password_from_url(channel_index_settings["server"])
        if "password" in channel_index_settings:
            channel_index_settings["password"] = "******"

def remove_passwords(json_text):
    """
    Here is an example of a content postprocessor that
    strips out all of the sensitive passwords
    """
    try:

        # lower case everything so that "databases" works as a key even if the JSON has "Databases"
        # as a key.  seems like there has to be a better way!
        json_text = json_text.lower()

        valid_json = convert_to_valid_json(json_text)

        parsed_json = json.loads(valid_json)

        # if only a database config fragment was passed rather than a full SG config
        # there will be top level "server" and "password" elements that need to be
        # patches
        remove_passwords_from_db_config(parsed_json)

        if "databases" in parsed_json:
            databases = parsed_json["databases"]
            for key, database in databases.iteritems():
                remove_passwords_from_db_config(database)


        formatted_json_string = json.dumps(parsed_json, indent=4)

        return formatted_json_string

    except Exception as e:
        msg = "Exception trying to remove passwords from {0}.  Exception: {1}".format(json_text, e)
        print(msg)
        traceback.print_exc()
        return '{"Error":"Error in sgcollect_info password_remover.py trying to remove passwords.  See logs for details"}'

def strip_password_from_url(url_string):
    """
    Given a URL string like:

    http://bucket-1:foobar@localhost:8091

    Strip out the password and return:

    http://bucket-1:@localhost:8091

    """

    parsed_url = urlparse(url_string)
    if parsed_url.username == None and parsed_url.password == None:
       return url_string

    new_url = "{0}://{1}:*****@{2}:{3}/{4}".format(
        parsed_url.scheme,
        parsed_url.username,
        parsed_url.hostname,
        parsed_url.port,
        parsed_url.query
    )
    return new_url

def escape_json_value(raw_value):
    """
    Escape all invalid json characters like " to produce a valid json value

    Before:

    function(doc, oldDoc) {            if (doc.type == "reject_me") {

    After:

    function(doc, oldDoc) {            if (doc.type == \"reject_me\") {

    """
    escaped = raw_value
    escaped = escaped.replace('\\', "\\\\")  # Escape any backslashes
    escaped = escaped.replace('"', '\\"')    # Escape double quotes
    escaped = escaped.replace("'", "\\'")    # Escape single quotes

    # TODO: other stuff should be escaped like \n \t and other control characters
    # See http://stackoverflow.com/questions/983451/where-can-i-find-a-list-of-escape-characters-required-for-my-json-ajax-return-ty

    return escaped

def convert_to_valid_json(invalid_json):

    """
    Find multiline string wrapped in backticks (``) and convert to a single line wrapped in double quotes
    """

    # is it already valid json?
    if is_valid_json(invalid_json):
        return invalid_json

    # remove all newlines to simplify our regular expression.  if newlines were left in, then
    # it would need to take that into account since '.' will only match any character *except* newlines
    no_newlines = invalid_json.replace('\n', '')

    # is it valid json after stripping newlines?
    if is_valid_json(no_newlines):
        return no_newlines

    # (.*)     any characters - group 0
    # `(.*)`   any characters within backquotes - group 1 -- what we want
    # (.*)     any characters - group 2
    regex_expression = '(.*)`(.*)`(.*)'

    match = re.match(regex_expression, no_newlines)
    if match is None:
        raise Exception("Was not valid JSON, but could not find a sync function enclosed in backquotes.")

    groups = match.groups()
    if groups is None or len(groups) != 3:
        raise Exception("Was not valid JSON, but could not find a sync function enclosed in backquotes.")

    # The text of the sync function will be in the 2nd group
    sync_function_text = groups[1]

    # Escape double quotes etc so that it's a valid json value
    sync_function_escaped = escape_json_value(sync_function_text)

    result = "{0}\"{1}\" {2}".format(
        groups[0],
        sync_function_escaped,
        groups[2]
    )

    return result

class TestStripPasswordsFromUrl(unittest.TestCase):

    def basic_test(self):
        url_with_password = "http://bucket-1:foobar@localhost:8091"
        url_no_password = strip_password_from_url(url_with_password)
        assert "foobar" not in url_no_password
        assert "bucket-1" in url_no_password

class TestRemovePasswords(unittest.TestCase):

    def test_basic(self):
        json_with_passwords = """
        {
          "log": ["*"],
          "databases": {
            "db2": {
                "server": "http://bucket-1:foobar@localhost:8091",
                "shadow": {
                    "server": "http://bucket2:foobar@localhost:8091"
                },
                "channel_index": {
                    "server": "http://bucket3:foobar@localhost:8091"
                }
            },
            "db": {
              "server": "http://localhost:8091",
              "bucket":"bucket-1",
              "username":"bucket-1",
              "password":"foobar",
              "users": { "GUEST": { "disabled": false, "admin_channels": ["*"] } },
              "sync":
            `
              function(doc, oldDoc) {
                if (doc.type == "reject_me") {
                  throw({forbidden : "Rejected document"})
                } else if (doc.type == "bar") {
              // add "bar" docs to the "important" channel
                    channel("important");
            } else if (doc.type == "secret") {
                  if (!doc.owner) {
                    throw({forbidden : "Secret documents \ must have an owner field"})
                  }
            } else {
                // all other documents just go into all channels listed in the doc["channels"] field
                channel(doc.channels)
            }
              }
            `
            }
          }
        }
        """
        with_passwords_removed = remove_passwords(json_with_passwords)
        assert "foobar" not in with_passwords_removed


    def test_alternative_config(self):

        sg_config = '{"Interface":":4984","AdminInterface":":4985","Facebook":{"Register":true},"Log":["*"],"Databases":{"todolite":{"server":"http://localhost:8091","pool":"default","bucket":"default","password":"foobar","name":"todolite","sync":"\\nfunction(doc, oldDoc) {\\n  // NOTE this function is the same across the iOS, Android, and PhoneGap versions.\\n  if (doc.type == \\"task\\") {\\n    if (!doc.list_id) {\\n      throw({forbidden : \\"Items must have a list_id.\\"});\\n    }\\n    channel(\\"list-\\"+doc.list_id);\\n  } else if (doc.type == \\"list\\" || (doc._deleted \\u0026\\u0026 oldDoc \\u0026\\u0026 oldDoc.type == \\"list\\")) {\\n    // Make sure that the owner propery exists:\\n    var owner = oldDoc ? oldDoc.owner : doc.owner;\\n    if (!owner) {\\n      throw({forbidden : \\"List must have an owner.\\"});\\n    }\\n\\n    // Make sure that only the owner of the list can update the list:\\n    if (doc.owner \\u0026\\u0026 owner != doc.owner) {\\n      throw({forbidden : \\"Cannot change owner for lists.\\"});\\n    }\\n\\n    var ownerName = owner.substring(owner.indexOf(\\":\\")+1);\\n    requireUser(ownerName);\\n\\n    var ch = \\"list-\\"+doc._id;\\n    if (!doc._deleted) {\\n      channel(ch);\\n    }\\n\\n    // Grant owner access to the channel:\\n    access(ownerName, ch);\\n\\n    // Grant shared members access to the channel:\\n    var members = !doc._deleted ? doc.members : oldDoc.members;\\n    if (Array.isArray(members)) {\\n      var memberNames = [];\\n      for (var i = members.length - 1; i \\u003e= 0; i--) {\\n        memberNames.push(members[i].substring(members[i].indexOf(\\":\\")+1))\\n      };\\n      access(memberNames, ch);\\n    }\\n  } else if (doc.type == \\"profile\\") {\\n    channel(\\"profiles\\");\\n    var user = doc._id.substring(doc._id.indexOf(\\":\\")+1);\\n    if (user !== doc.user_id) {\\n      throw({forbidden : \\"Profile user_id must match docid.\\"});\\n    }\\n    requireUser(user);\\n    access(user, \\"profiles\\");\\n  }\\n}\\n","users":{"GUEST":{"name":"","admin_channels":["*"],"all_channels":null,"disabled":true}}}}}'

        with_passwords_removed = remove_passwords(sg_config)

        assert "foobar" not in with_passwords_removed

    def test_config_fragment(self):
        db_config = """
        {
            "username": "bucket1",
            "name": "db",
            "bucket": "bucket1",
            "server": "http://localhost:8091",
            "password": "foobar",
            "pool": "default",
            "shadow": {
                "server": "http://bucket2:foobar@localhost:8091"
            },
            "channel_index": {
                "server": "http://bucket3:foobar@localhost:8091"
            }
        }
        """
        with_passwords_removed = remove_passwords(db_config)
        assert "foobar" not in with_passwords_removed
        pass

    def test_non_parseable_config(self):
        """
        If a config is not JSON parseable, make sure passwords are not stored in result
        """
        unparseable_json_with_passwords = """
        {
          "log": ["*"],
          "databases": {
            "db2": {
                "server": "http://bucket-1:foobar@localhost:8091",
                "shadow": {
                    "server": "http://bucket2:foobar@localhost:8091"
                },
                "channel_index": {
                    "server": "http://bucket3:foobar@localhost:8091"
                }
            },
            "db": {
              "server": "http://localhost:8091",
              "bucket":"bucket-1",
              "username":"bucket-1",
              "password":"foobar",
              "users": { "GUEST": { "disabled": false, "admin_channels": ["*"] } },
              "sync":

              function(doc, oldDoc) {
                if (doc.type == "reject_me") {
                  throw({forbidden : "Rejected document"})
                } else if (doc.type == "bar") {
              // add "bar" docs to the "important" channel
                    channel("important");
            } else if (doc.type == "secret") {
                  if (!doc.owner) {
                    throw({forbidden : "Secret documents \ must have an owner field"})
                  }
            } else {
                // all other documents just go into all channels listed in the doc["channels"] field
                channel(doc.channels)
            }
              }
            `
            }
          }
        }
        """
        with_passwords_removed = remove_passwords(unparseable_json_with_passwords)
        assert "foobar" not in with_passwords_removed



class TestConvertToValidJSON(unittest.TestCase):

    def basic_test(self):

        invalid_json = """
        {
          "log": ["*"],
          "databases": {
            "db": {
              "server": "walrus:",
              "users": { "GUEST": { "disabled": false, "admin_channels": ["*"] } },
              "sync":
            `
              function(doc, oldDoc) {
                if (doc.type == "reject_me") {
                  throw({forbidden : "Rejected document"})
                } else if (doc.type == "bar") {
              // add "bar" docs to the "important" channel
                    channel("important");
            } else if (doc.type == "secret") {
                  if (!doc.owner) {
                    throw({forbidden : "Secret documents \ must have an owner field"})
                  }
            } else {
                // all other documents just go into all channels listed in the doc["channels"] field
                channel(doc.channels)
            }
              }
            `
            }
          }
        }
        """

        valid_json = convert_to_valid_json(invalid_json)

        got_exception = True
        try:
            parsed_json = json.loads(valid_json)
            formatted_json_string = json.dumps(parsed_json, indent=4)
            got_exception = False
        except Exception as e:
            pass

        assert got_exception == False, "Failed to convert to valid JSON"


if __name__=="__main__":
    unittest.main()