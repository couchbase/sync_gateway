/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

function() {    var userFn = (%s);  // <-- substitutes the JS function

    if (typeof(userFn) !== "function")
        throw "The JavaScript code is not a function";

    function unmarshal(v) {return (typeof(v)==='string') ? JSON.parse(v) : v;}

    // This is what's passed as the `context` parameter to userFn:
    var Context = {
        user: {
            name: "",
            roles: [],
            channels: [],
            defaultCollection: {
                get:    function(docID)         {return unmarshal(_get(docID));},
                save:   function(docID, body)   {return _save(docID, body);},
                delete: function(docID, body)   {return _delete(docID, body);},
            },
            function:   function(name, args)    {return unmarshal(_func(name, args));},
            graphql:    function(q,args)        {return unmarshal(_graphql(q,args));},
        },
        admin: {
            defaultCollection: {
                get:    function(docID)         {return unmarshal(_get(docID, true));},
                save:   function(docID, body)   {return _save(docID, body, true);},
                delete: function(docID, body)   {return _delete(docID, body, true);},
            },
            function:   function(name, args)    {return unmarshal(_func(name, args, true));},
            graphql:    function(q,args)        {return unmarshal(_graphql(q,args, true));},
        },

        requireAdmin: function() {
            if (!this.user.name)  return;  // user is admin
            throw("HTTP: 403 Forbidden");
        },

        requireUser: function(name) {
            var userName = this.user.name;
            if (!userName)  return;  // user is admin
            var allowed;
            if (Array.isArray(name)) {
                allowed = (name.indexOf(userName) != -1);
            } else {
                allowed = (userName == name);
            }
            if (!allowed)
                throw("HTTP: 401 Unauthorized");
        },

        requireRole: function(role) {
            var userRoles = this.user.roles;
            if (!userRoles)  return;  // user is admin
            if (Array.isArray(role)) {
                for (var i = 0; i < role.length; ++i) {
                    if (userRoles[role[i]] !== undefined)
                        return;
                }
            } else {
                if (userRoles[role] !== undefined)
                    return;
            }
            throw("HTTP: 401 Unauthorized");
        },

        requireAccess: function(channel) {
            var userChannels = this.user.channels;
            if (!userChannels)  return;  // user is admin
            if (Array.isArray(channel)) {
                for (var i = 0; i < channel.length; ++i) {
                    if (userChannels.indexOf(channel[i]) != -1)
                        return;
                }
            } else {
                if (userChannels.indexOf(channel) != -1)
                    return;
            }
            throw("HTTP: 401 Unauthorized");
        }
    };


    // Standard JS function not implemented in Otto
    if (!Array.from) {
        Array.from = function(v) {
            var len = v.length;
            if (typeof(len) !== 'number') throw TypeError("Array.from")
            var a = new Array(len);
            for (i = 0; i < len; ++i)
                a[i] = v[i];
            return a;
        }
    }


    // Return the JS function that will be invoked repeatedly by the runner:
    return function (userInfo, p1, p2, p3, p4) {
        var context = Object.create(Context);
        context.user = Object.create(Context.user);
        context.user.name = userInfo.name;
        context.user.roles = userInfo.roles;
        context.user.channels = userInfo.channels;
        return userFn(context, p1, p2, p3, p4);
    };
}()
