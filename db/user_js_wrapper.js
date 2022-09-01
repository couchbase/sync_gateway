function() {
    function userFn(%[1]s) {  // <-- substitutes the parameter list of the JS function
        %[2]s                 // <-- substitutes the actual JS code from the config file
    }

    // Prototype of the user object:
    function User(info) {
        this.name = info.name;
        this.roles = info.roles;
        this.channels = info.channels;
    }

    User.prototype.requireAdmin = function() {
        throw("HTTP: 403 Forbidden");
    }

    User.prototype.requireName = function(name) {
        var allowed;
        if (Array.isArray(name)) {
            allowed = name.indexOf(this.name) != -1;
        } else {
            allowed = this.name == name;
        }
        if (!allowed)
            throw("HTTP: 401 Unauthorized");
    }

    User.prototype.requireRole = function(role) {
        if (Array.isArray(role)) {
            for (var i = 0; i < role.length; ++i) {
                if (this.roles[role[i]] !== undefined)
                    return;
            }
        } else {
            if (this.roles[role] !== undefined)
                return;
        }
        throw("HTTP: 401 Unauthorized");
    }

    User.prototype.requireAccess = function(channel) {
        if (Array.isArray(channel)) {
            for (var i = 0; i < channel.length; ++i) {
                if (this.channels.indexOf(channel[i]) != -1)
                    return;
            }
        } else {
            if (this.channels.indexOf(channel) != -1)
                return;
        }
        throw("HTTP: 401 Unauthorized");
    }

    // Admin prototype makes all the "require..." functions no-ops:
    function Admin() { }
    Admin.prototype.requireAdmin = Admin.prototype.requireName =
        Admin.prototype.requireRole = Admin.prototype.requireAccess = function() { }

    function MakeUser(info) {
        if (info && info.name !== undefined) {
            return new User(info);
        } else {
            return new Admin();
        }
    }

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

    function unmarshal(v) {return (typeof(v)==='string') ? JSON.parse(v) : v;}

    // App object contains the native Go functions to access the database:
    var App = {
        func:    function(name, args){return unmarshal(_func(name, args));},
        get:     function(docID)     {return unmarshal(_get(docID));},
        graphql: function(q,args)    {return unmarshal(_graphql(q,args));},
        query:   function(name,args) {return unmarshal(_query(name,args));},
        save:    _save
    };

    // Return the JS function that will be invoked repeatedly by the runner:
    return function (%[1]s) {
        context.user = MakeUser(context.user);
        context.app = Object.create(App);
        return userFn(%[1]s);
    };
}()
