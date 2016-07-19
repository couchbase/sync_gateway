"""
Removes passwords from config files

"""


import unittest
import json
import re

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

    sync_function_text = groups[1]
    print sync_function_text
    sync_function_escaped = escape_json_value(sync_function_text)
    print sync_function_escaped

    # result =  "{}\"\"{}".format(groups[0], groups[2])
    result = groups[0] + '"' + sync_function_escaped + '"' + " " + groups[2]
    # print("result: {}".format(result))
    print "result: " + result
    return result

class TestConvertToValidJSON(unittest.TestCase):

    valid_json = """
    {
      "log": ["*"],
      "databases": {
        "db": {
          "server": "walrus:",
          "users": { "GUEST": { "disabled": false, "admin_channels": ["*"] } },
          "sync":"function(doc, oldDoc) { if (doc.type == \\"reject_me\\") { throw({forbidden : \\"Rejected document\\"}) } else if (doc.type == \\"bar\\") { // add \\"bar\\" docs to the \\"important\\" channel channel(\\"important\\"); } else if (doc.type == \\"secret\\") { if (!doc.owner) { throw({forbidden : \\"Secret documents must have an owner field\\"}) } } else { // all other documents just go into all channels listed in the doc[\\"channels\\"] field channel(doc.channels) } }"
        }
      }
    }
    """

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
        print formatted_json_string
        got_exception = False
    except Exception as e:
        pass

    assert got_exception == False, "Failed to convert to valid JSON"
#
# def remove_passwords(config_str):
#
#     # look for regexes like
#     # "password":"foobar"
#
#     # look for regexes like
#     # "server":"http://bucket-2:foobar@localhost:8091"
#
#     return config_str
#
# class TestRemovePasswords(unittest.TestCase):
#
#     def test_remove_passwords(self):
#         config_str = """
#         {
#            "log":[
#               "*"
#            ],
#            "databases":{
#               "db":{
#                  "server":"http://localhost:8091",
#                  "bucket":"bucket-1",
#                  "username":"bucket-1",
#                  "password":"foobar"
#               },
#               "db2":{
#                  "server":"http://bucket-2:foobar@localhost:8091"
#               }
#            }
#         }
#         """
#         passwords_removed = remove_passwords(config_str)
#         assert "foobar" not in passwords_removed


if __name__=="__main__":
    unittest.main()