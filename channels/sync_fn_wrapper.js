	(function() {

		var realUserCtx, shouldValidate;
		var syncFn = %s;

		let log = console.log;

		function makeArray(maybeArray) {
			if (Array.isArray(maybeArray)) {
				return maybeArray;
			} else {
				return [maybeArray];
			}
		}

		function inArray(string, array) {
			return array.indexOf(string) != -1;
		}

		function anyInArray(any, array) {
			for (var i = 0; i < any.length; ++i) {
				if (inArray(any[i], array))
					return true;
			}
			return false;
		}

		function anyKeysInArray(any, array) {
			for (var key in any) {
				if (inArray(key, array))
					return true;
			}
			return false;
		}

		function requireAdmin() {
			if (shouldValidate)
				throw({forbidden: "%s"});
		}

		function requireUser(names) {
				if (!shouldValidate) return;
				names = makeArray(names);
				if (!inArray(realUserCtx.name, names))
					throw({forbidden: "%s"});
		}

		function requireRole(roles) {
				if (!shouldValidate) return;
				roles = makeArray(roles);
				if (!anyKeysInArray(realUserCtx.roles, roles))
					throw({forbidden: "%s"});
		}

		function requireAccess(channels) {
				if (!shouldValidate) return;
				channels = makeArray(channels);
				if (!anyInArray(realUserCtx.channels, channels))
					throw({forbidden: "%s"});
		}

		// This is the function that's called to run the sync function:
		return function (newDoc, oldDoc, meta, _realUserCtx) {
			if (oldDoc) {
				oldDoc._id = newDoc._id
			}

			realUserCtx = _realUserCtx;

			// Proxy userCtx that allows queries but not direct access to user/roles:
			shouldValidate = (realUserCtx != null && realUserCtx.name != null);

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
		}
	}())
