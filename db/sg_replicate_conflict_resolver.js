function() { var conflictResolver = %s;

//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

    function defaultPolicy(conflict) {
        function chk(obj) {
            if (!obj || typeof(obj) !== "object")
                throw "defaultPolicy() called with invalid conflict object";
            return obj;
        }

        // Based on the Go function DefaultConflictResolver()
        chk(conflict);
        var local = chk(conflict.LocalDocument);
        var remote = chk(conflict.RemoteDocument);
        var localDeleted = local._deleted;
        var remoteDeleted = remote._deleted;
        if (localDeleted && !remoteDeleted)  return local;
        if (remoteDeleted && !localDeleted)  return remote;

        var rev1 = local._rev.split('-', 2);
        var rev2 = remote._rev.split('-', 2);
        var gen1 = Number(rev1[0]);
        var gen2 = Number(rev2[0]);
        if (gen1 < gen2 || (gen1 == gen2 && rev1[1] < rev2[1]))
            return remote;
        else
            return local;
    }

    return conflictResolver;
}()
