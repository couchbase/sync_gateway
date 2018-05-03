"""
Redacts sensitive data in config files

"""


import unittest
import json
from urlparse import urlparse
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

    return got_exception is False


def tag_userdata_in_server_config(json_text, log_json_parsing_exceptions=True):
    """
    Content postprocessor that tags user data in a config ready for post-process redaction
    """
    try:

        # lower case everything so that "databases" works as a key even if the JSON has "Databases"
        # as a key.  seems like there has to be a better way!
        json_text = json_text.lower()
        valid_json = convert_to_valid_json(json_text)
        parsed_json = json.loads(valid_json)

        tag_userdata_in_server_json(parsed_json)

        formatted_json_string = json.dumps(parsed_json, indent=4)
        return formatted_json_string

    except Exception as e:
        if log_json_parsing_exceptions:
            print("Exception trying to tag config user data in {0}.  Exception: {1}".format(json_text, e))
            traceback.print_exc()
        return '{"Error":"Error in sgcollect_info password_remover.py trying to tag config user data.  See logs for details"}'


def tag_userdata_in_server_json(config):
        """
        Given a dictionary that contains a full set of configuration values:
        - Tag any sensitive user-data fields with <ud></ud> tags.
        """

        if "databases" in config:
            dbs = config["databases"]
            for db in dbs:
                tag_userdata_in_db_json(dbs[db])


def tag_userdata_in_db_config(json_text, log_json_parsing_exceptions=True):
    """
    Content postprocessor that tags user data in a db config ready for post-process redaction
    """
    try:

        # lower case everything so that "databases" works as a key even if the JSON has "Databases"
        # as a key.  seems like there has to be a better way!
        json_text = json_text.lower()
        valid_json = convert_to_valid_json(json_text)
        parsed_json = json.loads(valid_json)

        tag_userdata_in_db_json(parsed_json)

        formatted_json_string = json.dumps(parsed_json, indent=4)
        return formatted_json_string

    except Exception as e:
        if log_json_parsing_exceptions:
            print("Exception trying to tag db config user data in {0}.  Exception: {1}".format(json_text, e))
            traceback.print_exc()
        return '{"Error":"Error in sgcollect_info password_remover.py trying to tag db config user data.  See logs for details"}'


def tag_userdata_in_db_json(db):
        """
        Given a dictionary that contains a set of db configuration values:
        - Tag any sensitive user-data fields with <ud></ud> tags.
        """

        if "username" in db:
            db["username"] = UD(db["username"])

        if "users" in db:
            users = db["users"]
            for username in users:
                user = users[username]
                if "name" in user:
                    user["name"] = UD(user["name"])
                if "admin_channels" in user:
                    admin_channels = user["admin_channels"]
                    for i, _ in enumerate(admin_channels):
                        admin_channels[i] = UD(admin_channels[i])
                if "admin_roles" in user:
                    admin_roles = user["admin_roles"]
                    for i, _ in enumerate(admin_roles):
                        admin_roles[i] = UD(admin_roles[i])
            # Tag dict keys. Can't be done in the above loop.
            for i, _ in users.items():
                users[UD(i)] = users.pop(i)

        if "roles" in db:
            roles = db["roles"]
            for rolename in roles:
                role = roles[rolename]
                if "admin_channels" in role:
                    admin_channels = role["admin_channels"]
                    for i, _ in enumerate(admin_channels):
                        admin_channels[i] = UD(admin_channels[i])
            # Tag dict keys. Can't be done in the above loop.
            for i, _ in roles.items():
                roles[UD(i)] = roles.pop(i)


def UD(value):
    """
    Tags the given value with User Data tags.
    """
    return "<ud>{0}</ud>".format(value)


def remove_passwords_from_config(config_fragment):
    """
    Given a dictionary that contains configuration values, recursively walk the dictionary and:

    - Replace any fields w/ key "password" with "*****"
    - Replace any fields w/ key "server" with the result of running it through strip_password_from_url()
    """

    if not isinstance(config_fragment, dict):
        return

    if "server" in config_fragment:
        config_fragment["server"] = strip_password_from_url(config_fragment["server"])
    if "password" in config_fragment:
        config_fragment["password"] = "******"

    for key, item in config_fragment.items():
        if isinstance(item, dict):
            remove_passwords_from_config(item)


def remove_passwords(json_text, log_json_parsing_exceptions=True):
    """
    Content postprocessor that strips out all of the sensitive passwords
    """
    try:

        # lower case everything so that "databases" works as a key even if the JSON has "Databases"
        # as a key.  seems like there has to be a better way!
        json_text = json_text.lower()

        valid_json = convert_to_valid_json(json_text)

        parsed_json = json.loads(valid_json)

        remove_passwords_from_config(parsed_json)

        formatted_json_string = json.dumps(parsed_json, indent=4)

        return formatted_json_string

    except Exception as e:
        if log_json_parsing_exceptions:
            print("Exception trying to remove passwords from {0}.  Exception: {1}".format(json_text, e))
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
    if parsed_url.username is None and parsed_url.password is None:
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

    STATE_OUTSIDE_BACKTICK = "STATE_OUTSIDE_BACKTICK"
    STATE_INSIDE_BACKTICK = "STATE_INSIDE_BACKTICK"
    state = STATE_OUTSIDE_BACKTICK
    output = []
    sync_function_buffer = []

    # Strip newlines
    invalid_json = invalid_json.replace('\n', '')

    # Strip tabs
    invalid_json = invalid_json.replace('\t', '')

    # read string char by char
    for json_char in invalid_json:

        # if non-backtick character:
        if json_char != '`':

            # if in OUTSIDE_BACKTICK state
            if state == STATE_OUTSIDE_BACKTICK:
                # append char to output
                output.append(json_char)

            # if in INSIDE_BACKTICK state
            elif state == STATE_INSIDE_BACKTICK:
                # append to sync_function_buffer
                sync_function_buffer.append(json_char)

        # if backtick character
        elif json_char == '`':

            # if in OUTSIDE_BACKTICK state
            if state == STATE_OUTSIDE_BACKTICK:
                # transition to INSIDE_BACKTICK state
                state = STATE_INSIDE_BACKTICK

            # if in INSIDE_BACKTICK state
            elif state == STATE_INSIDE_BACKTICK:
                # run sync_function_buffer through escape_json_value()
                sync_function_buffer_str = "".join(sync_function_buffer)
                sync_function_buffer_str = escape_json_value(sync_function_buffer_str)

                # append to output
                output.append('"')  # append a double quote
                output.append(sync_function_buffer_str)
                output.append('"')  # append a double quote

                # empty the sync_function_buffer
                sync_function_buffer = []

                # transition to OUTSIDE_BACKTICK state
                state = STATE_OUTSIDE_BACKTICK

    output_str = "".join(output)
    return output_str


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
        with_passwords_removed = remove_passwords(unparseable_json_with_passwords, log_json_parsing_exceptions=False)
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
        tagged = tag_userdata_in_server_config(json_with_userdata)
        assert "<ud>uber_secret_channel</ud>" in tagged
        assert "<ud>foo</ud>" in tagged           # everything is lowercased
        assert "<ud>bucket-user</ud>" in tagged

        assert "<ud>baz</ud>" not in tagged       # passwords shouldn't be tagged, they get removed
        assert "<ud>bucket-1</ud>" not in tagged  # bucket name is acutally metadata


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
            json.dumps(parsed_json, indent=4)
            got_exception = False
        except Exception as e:
            print("Exception: {0}".format(e))

        assert got_exception is False, "Failed to convert to valid JSON"

    def basic_test_two_sync_functions(self):

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


if __name__ == "__main__":
    unittest.main()
