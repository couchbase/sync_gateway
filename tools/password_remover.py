"""
Removes passwords from config files

"""


import unittest
import json

def convert_to_valid_json(invalid_json):

    """
    Find multiline string wrapped in backticks (``) and convert to a single line wrapped in double quotes
    """

    return invalid_json


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
                throw({forbidden : "Secret documents must have an owner field"})
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
        json.loads(valid_json)
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