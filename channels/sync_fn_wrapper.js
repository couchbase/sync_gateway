//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

function() {
    const syncFn = %s;

    if (typeof(syncFn) !== 'function') {
        throw new Error(`code does not compile to a function`);
    } else if (syncFn.length < 1 || syncFn.length > 3) {
        throw new Error(`sync function must have 1-3 arguments`);
    }

    {
        /****  Variables used during the call but not visible to the sync fn ****/

        let userCtx;		// object with keys .name, .roles, .channels
        let shouldValidate;	// if false, 'require...' functions are no-ops
        let result;			// object to be returned, with keys .channels, .access


        /**** Utility functions ****/

        // Returns true if `what` is equal to `against` or included in it.
        function match(what /*:string | string[]*/, against /*:string*/) {
            if (Array.isArray(what)) {
                return what.includes(against);
            } else {
                return (what == against);
            }
        }

        // Returns true if `what` is a key of `against`, or an array that contains a key of it.
        function matchInObject(what /*:string | string[]*/, against /*:object*/) {
            if (Array.isArray(what)) {
                for (let w of what) {
                    if (against.hasOwnProperty(w)) return true;
                }
                return false;
            } else {
                return against.hasOwnProperty(what);
            }
        }

        // Returns true if `what` is contained in `against`,
        // or an array that contains an item of `against`.
        function matchInArray(what /*:string | string[]*/, against /*:string[]*/) {
            if (Array.isArray(what)) {
                for (let w of what) {
                    if (against.includes(w)) return true;
                }
                return false;
            } else {
                return against.includes(what);
            }
        }

        // Appends a string or array of strings `what` to an array `list`.
        function appendTo(what /*:string | string[]*/, list /*:string[]*/) {
            if (typeof(what) === 'string') {
                list.push(what)
            } else if (Array.isArray(what)) {
                for (let c of what) {
                    appendTo(c, list);
                }
            } else {
                console.warn(`Ignoring non-string channel or role name ${what}`);
            }
        }

        // Appends `items` (a string or array of strings) to the array `map[user]`.
        function _appendForUser(map, user, items) {
            if (typeof(user) !== 'string') {
                console.warn(`Ignoring non-string username ${user}`);
                return;
            }
            let userAccess = map[user];
            if (userAccess === undefined) {
                userAccess = map[user] = [];
            }
            appendTo(items, userAccess);
        }

        // Same as _appendForUser, but `user` can be an array of user names.
        function appendForUser(map, user, items) {
            if (Array.isArray(user)) {
                for (let u of user)
                    _appendForUser(map, u, items);
            } else {
                _appendForUser(map, user, items);
            }
        }


        /**** Validation functions ****/

        globalThis.requireAdmin = function() {
            if (shouldValidate)
                throw({forbidden: "%s"});
        }

        globalThis.requireUser = function(nameOrNames) {
            if (shouldValidate && !match(nameOrNames, userCtx.name))
                throw({forbidden: "%s"});
        }

        globalThis.requireRole = function(roles) {
            if (shouldValidate && !matchInObject(roles, userCtx.roles))
                throw({forbidden: "%s"});
        }

        globalThis.requireAccess = function(channels) {
            if (shouldValidate && !matchInArray(channels, userCtx.channels))
                throw({forbidden: "%s"});
        }


        /**** Access grant functions ****/

        globalThis.channel = function(...channels) {
            appendTo(channels, result.channels)
        }

        globalThis.access = function(user, channels) {
            if (result.access === undefined) result.access = {}
            appendForUser(result.access, user, channels);
        }

        globalThis.role = function(user, roles) {
            if (result.roles === undefined) result.roles = {}
            appendForUser(result.roles, user, roles);
        }

        globalThis.expiry = function(x) {
            if (typeof(x) === 'number' || typeof(x) === 'string')
                result.expiry = x;
        }

        globalThis.reject = function(status, message) {
            result.rejectionStatus = status;
            result.rejectionMessage = message;
        }

        globalThis.log = console.log;

        // Prevent scripts from dynamically generating code:
        delete globalThis.eval;
        delete globalThis.Function;

        /**** The function that runs the sync function ****/

        return function (newDoc, oldDoc, meta, _userCtx) {
            if (oldDoc) {
                oldDoc._id = newDoc._id
            }

            userCtx = _userCtx;
            shouldValidate = (userCtx != null && userCtx.name != null);
            result = { channels: [] };

            try {
                syncFn(newDoc, oldDoc, meta);
            } catch(x) {
                if (x.forbidden)
                    reject(403, x.forbidden);
                else if (x.unauthorized)
                    reject(401, x.unauthorized);
                else
                    throw(x);
            }

            // Go has trouble getting arbitrary keys from V8 objects, so help it out:
            if (result.access !== undefined)
                result.accessKeys = Object.getOwnPropertyNames(result.access);
            if (result.roles !== undefined)
                result.rolesKeys = Object.getOwnPropertyNames(result.roles);
            return result;
        }
    }
}()
