"""
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

"""
Redacts sensitive data in config files

"""

import enum
import json
import traceback
import re
from urllib.parse import urlparse

from typing import Union

def get_parsed_json(json_text: str) -> dict:
    """
    Turns json_text into a valid JSON object by replacing backquotes with single quotes, and returns it as a dictionary.
    """
    valid_json = convert_to_valid_json(json_text)

    # Lower case keys so that "databases" works as a
    # key even if the JSON has "Databases" as a key.
    return lower_keys_dict(valid_json)


def tag_userdata_in_server_config(json_text):
    """
    Content postprocessor that tags user data in a config ready for post-process redaction
    """
    try:
        parsed_json = get_parsed_json(json_text)

        tag_userdata_in_server_json(parsed_json)
        formatted_json_string = json.dumps(parsed_json, indent=4)
        return formatted_json_string

    except Exception as e:
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


def tag_userdata_in_db_config(json_text):
    """
    Content postprocessor that tags user data in a db config ready for post-process redaction
    """
    try:
        valid_json = convert_to_valid_json(json_text)

        # Lower case keys so that "databases" works as a
        # key even if the JSON has "Databases" as a key.
        parsed_json = lower_keys_dict(valid_json)

        tag_userdata_in_db_json(parsed_json)
        formatted_json_string = json.dumps(parsed_json, indent=4)
        return formatted_json_string

    except Exception as e:
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
            for i, _ in list(users.items()):
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
            for i, _ in list(roles.items()):
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

    for key, item in list(config_fragment.items()):
        if isinstance(item, dict):
            remove_passwords_from_config(item)


def remove_passwords(json_text):
    """
    Content postprocessor that strips out all of the sensitive passwords
    """
    try:
        parsed_json = get_parsed_json(json_text)

        remove_passwords_from_config(parsed_json)

        # Append a trailing \n here to ensure there's adequate separation in sync_gateway.log
        formatted_json_string = json.dumps(parsed_json, indent=4) + "\n"
        return formatted_json_string

    except Exception as e:
        print("Exception trying to remove passwords from {0}.  Exception: {1}".format(json_text, e))
        traceback.print_exc()
        return '{"Error":"Error in sgcollect_info password_remover.py trying to remove passwords.  See logs for details"}'


def lower_keys_dict(json_text):
    """Deserialize the given JSON document to a Python dictionary and
    transform all keys to lower case.
    """
    def iterate(k):
        return lower_level(k) if isinstance(k, dict) else k

    def lower(k):
        return k.lower() if isinstance(k, str) else k

    def lower_level(kv):
        return dict((lower(k), iterate(v)) for k, v in kv.items())

    json_dict = json.loads(json_text)
    return lower_level(json_dict)


def pretty_print_json(json_text):
    """
    Content postprocessor that pretty prints JSON.
    Returns original string with a trailing \n (to ensure separation in sync_gateway.log) if formatting fails
    """
    try:
        json_text = json.dumps(json.loads(json_text), indent=4)
    except Exception as e:
        print("Exception trying to parse JSON {0}.  Exception: {1}".format(json_text, e))
    return json_text + "\n"


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


def replace_backticks_with_double_quotes(match):
    """
    Escape all invalid json characters in sync gateway config files that occur between backticks to produce a valid json value. This matches ConvertBackticksToDoubleQuotes in go code.

    Before::

        function(doc, oldDoc) { if (doc.type == "reject_me") {

    After::

        function(doc, oldDoc) { if (doc.type == \"reject_me\") {

    """

    replacements = [
            ("\\", "\\\\"), # replace literal slash with two slashes
            ("\r\n", "\\n"),
            ("\r", ""),
            ("\n", "\\n"),
            ("\t", "\\t"),
            (r'"', r'\"')
    ]
    expr = match.group(0)
    for search, replace in replacements:
        expr = expr.replace(search, replace)

    # Replace the backquotes with double-quotes
    expr = '"' + expr[1:]
    expr = expr[:-1] + '"'
    return expr

def convert_to_valid_json(invalid_json: Union[bytes,str]) -> str:
    """
    Converts json text from a sync gateway config file to valid json by replacing backticks with double quotes and escaping invalid json characters inside the backquotes.
    """
    if isinstance(invalid_json, bytes):
        invalid_json = invalid_json.decode('utf-8', errors="backslashreplace")
    return re.sub(r"`(.*?)[^\\\\]`", replace_backticks_with_double_quotes, invalid_json, flags=re.MULTILINE|re.DOTALL)
