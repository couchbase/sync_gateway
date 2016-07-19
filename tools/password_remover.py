"""
Removes passwords from config files

"""


import unittest
import json
import re
from urlparse import urlparse, urlunparse

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


def remove_passwords(json_text):
    """
    Here is an example of a content postprocessor that
    strips out all of the sensitive passwords
    """

    valid_json = convert_to_valid_json(json_text)

    parsed_json = json.loads(valid_json)

    databases = parsed_json["databases"]
    for key, database in databases.iteritems():
        if "server" in database:
            database["server"] = strip_password_from_url(database["server"])
        if "password" in database:
            database["password"] = "******"

    formatted_json_string = json.dumps(parsed_json, indent=4)

    return formatted_json_string

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

    # (.*)     any characters - group 0
    # `(.*)`   any characters within backquotes - group 1 -- what we want
    # (.*)     any characters - group 2
    regex_expression = '(.*)`(.*)`(.*)'

    groups = re.match(regex_expression, no_newlines).groups()
    if len(groups) != 3:
        raise Exception("Was not valid JSON, but could not find a sync function enclosed in backquotes.  Not sure what to do")

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

    url_with_password = "http://bucket-1:foobar@localhost:8091"
    url_no_password = strip_password_from_url(url_with_password)
    assert "foobar" not in url_no_password
    assert "bucket-1" in url_no_password


class TestRemovePasswords(unittest.TestCase):
    json_with_passwords = """
    {
      "log": ["*"],
      "databases": {
        "db2": {
            "server": "http://bucket-1:foobar@localhost:8091"
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


class TestConvertToValidJSON(unittest.TestCase):


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