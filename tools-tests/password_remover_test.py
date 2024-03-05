# Copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

import json
import unittest

import pytest

import password_remover

class TestStripPasswordsFromUrl(unittest.TestCase):

    def basic_test(self):
        url_with_password = "http://bucket-1:foobar@localhost:8091"
        url_no_password = strip_password_from_url(url_with_password)
        assert "foobar" not in url_no_password
        assert "bucket-1" in url_no_password


class TestRemovePasswords(unittest.TestCase):

    def test_basic(self):
        json_with_passwords = r"""
        {
          "log": ["*"],
          "databases": {
            "db2": {
                "server": "http://bucket-1:foobar@localhost:8091"
            },
            "db": {
              "server": "http://bucket4:foobar@localhost:8091",
              "bucket":"bucket-1",
              "username":"bucket-1",
              "password":"foobar",
              "users": { "Foo": { "password": "foobar", "disabled": false, "admin_channels": ["*"] } },
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
        with_passwords_removed = password_remover.remove_passwords(json_with_passwords)
        assert "foobar" not in with_passwords_removed

    def test_alternative_config(self):

        sg_config = '{"Interface":":4984","AdminInterface":":4985","Facebook":{"Register":true},"Log":["*"],"Databases":{"todolite":{"server":"http://localhost:8091","pool":"default","bucket":"default","password":"foobar","name":"todolite","sync":"\\nfunction(doc, oldDoc) {\\n  // NOTE this function is the same across the iOS, Android, and PhoneGap versions.\\n  if (doc.type == \\"task\\") {\\n    if (!doc.list_id) {\\n      throw({forbidden : \\"Items must have a list_id.\\"});\\n    }\\n    channel(\\"list-\\"+doc.list_id);\\n  } else if (doc.type == \\"list\\" || (doc._deleted \\u0026\\u0026 oldDoc \\u0026\\u0026 oldDoc.type == \\"list\\")) {\\n    // Make sure that the owner propery exists:\\n    var owner = oldDoc ? oldDoc.owner : doc.owner;\\n    if (!owner) {\\n      throw({forbidden : \\"List must have an owner.\\"});\\n    }\\n\\n    // Make sure that only the owner of the list can update the list:\\n    if (doc.owner \\u0026\\u0026 owner != doc.owner) {\\n      throw({forbidden : \\"Cannot change owner for lists.\\"});\\n    }\\n\\n    var ownerName = owner.substring(owner.indexOf(\\":\\")+1);\\n    requireUser(ownerName);\\n\\n    var ch = \\"list-\\"+doc._id;\\n    if (!doc._deleted) {\\n      channel(ch);\\n    }\\n\\n    // Grant owner access to the channel:\\n    access(ownerName, ch);\\n\\n    // Grant shared members access to the channel:\\n    var members = !doc._deleted ? doc.members : oldDoc.members;\\n    if (Array.isArray(members)) {\\n      var memberNames = [];\\n      for (var i = members.length - 1; i \\u003e= 0; i--) {\\n        memberNames.push(members[i].substring(members[i].indexOf(\\":\\")+1))\\n      };\\n      access(memberNames, ch);\\n    }\\n  } else if (doc.type == \\"profile\\") {\\n    channel(\\"profiles\\");\\n    var user = doc._id.substring(doc._id.indexOf(\\":\\")+1);\\n    if (user !== doc.user_id) {\\n      throw({forbidden : \\"Profile user_id must match docid.\\"});\\n    }\\n    requireUser(user);\\n    access(user, \\"profiles\\");\\n  }\\n}\\n","users":{"GUEST":{"name":"","admin_channels":["*"],"all_channels":null,"disabled":true}}}}}'

        with_passwords_removed = password_remover.remove_passwords(sg_config)

        assert "foobar" not in with_passwords_removed

    def test_config_fragment(self):
        db_config = """
        {
            "username": "bucket1",
            "name": "db",
            "bucket": "bucket1",
            "server": "http://localhost:8091",
            "password": "foobar",
            "pool": "default"
        }
        """
        with_passwords_removed = password_remover.remove_passwords(db_config)
        assert "foobar" not in with_passwords_removed
        pass

    def test_non_parseable_config(self):
        """
        If a config is not JSON parseable, make sure passwords are not stored in result
        """
        unparseable_json_with_passwords = r"""
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
        with_passwords_removed = password_remover.remove_passwords(unparseable_json_with_passwords)
        assert "foobar" not in with_passwords_removed


class TestTagUserData(unittest.TestCase):

    def test_basic(self):
        json_with_userdata = """
        {
          "databases": {
            "db": {
              "server": "http://bucket4:foobar@localhost:8091",
              "bucket":"bucket-1",
              "username":"bucket-user",
              "password":"foobar",
              "users": {
                "FOO": {
                  "password": "foobar",
                  "disabled": false,
                  "admin_channels": ["uber_secret_channel"]
                },
                "bar": { "password": "baz" }
              }
            },
            "db2": { "server": "http://bucket-1:foobar@localhost:8091" }
          }
        }
        """
        tagged = password_remover.tag_userdata_in_server_config(json_with_userdata)
        assert "<ud>uber_secret_channel</ud>" in tagged
        assert "<ud>foo</ud>" in tagged           # everything is lower cased
        assert "<ud>bucket-user</ud>" in tagged

        assert "<ud>baz</ud>" not in tagged       # passwords shouldn't be tagged, they get removed
        assert "<ud>bucket-1</ud>" not in tagged  # bucket name is actually metadata


class TestConvertToValidJSON(unittest.TestCase):

    def basic_test(self):

        invalid_json = r"""
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
            json.dumps(parsed_json, indent=4)
            got_exception = False
        except Exception as e:
            print("Exception: {0}".format(e))

        assert got_exception is False, "Failed to convert to valid JSON"

    def basic_test_two_sync_functions(self):

        invalid_json = r"""
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
                },
                "db2": {
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
                },
              }
            }
            """

        valid_json = convert_to_valid_json(invalid_json)

        got_exception = True
        try:
            parsed_json = json.loads(valid_json)
            json.dumps(parsed_json, indent=4)
            got_exception = False
        except Exception as e:
            print("Exception: {0}".format(e))

        assert got_exception is False, "Failed to convert to valid JSON"


class TestLowerKeys(unittest.TestCase):

    def test_basic(self):
        json_text_input = """{
           "Name": "Couchbase, Inc.",
           "Address": {
              "Street": "3250 Olcott St",
              "City": "Santa Clara",
              "State": "CA",
              "Zip_Code": 95054
           },
           "Products": [
              "Couchbase Server",
              "Sync Gateway",
              "Couchbase Lite"
           ]
        }"""
        json_dict_actual = password_remover.lower_keys_dict(json_text_input)
        json_dict_expected = json.loads("""{
           "name": "Couchbase, Inc.",
           "address": {
              "street": "3250 Olcott St",
              "city": "Santa Clara",
              "state": "CA",
              "zip_code": 95054
           },
           "products": [
              "Couchbase Server",
              "Sync Gateway",
              "Couchbase Lite"
           ]
        }""")

        # Sort the lists(if any) in both dictionaries before dumping to a string.
        json_text_actual = json.dumps(json_dict_actual, sort_keys=True)
        json_text_expected = json.dumps(json_dict_expected, sort_keys=True)
        assert json_text_expected == json_text_actual

@pytest.mark.parametrize("input_str, expected", [
       (
			b'{"foo": "bar"}',
			'{"foo": "bar"}',
		),
		(
			b'{\"foo\": `bar`}',
			'{"foo": "bar"}',
		),
		(
			b'{\"foo\": `bar\nbaz\nboo`}',
			r'{"foo": "bar\nbaz\nboo"}',
		),
		(
			b'{\"foo\": `bar\n\"baz\n\tboo`}',
			r'{"foo": "bar\n\"baz\n\tboo"}',
		),
		(
			b'{\"foo\": `bar\n`, \"baz\": `howdy`}',
			r'{"foo": "bar\n", "baz": "howdy"}',
		),
		(
			b'{\"foo\": `bar\r\n`, \"baz\": `\r\nhowdy`}',
			r'{"foo": "bar\n", "baz": "\nhowdy"}',
		),
		(
			b'{\"foo\": `bar\\baz`, \"something\": `else\\is\\here`}',
			r'{"foo": "bar\\baz", "something": "else\\is\\here"}',
		),
        ])
def test_convert_to_valid_json(input_str, expected):
    assert password_remover.convert_to_valid_json(input_str) == expected
    password_remover.get_parsed_json(input_str)
