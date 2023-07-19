/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// Apollo's library imports the URL class from Node's `url` package.
// Unfortunately there isn't a polyfill library that works exactly like this.
// The `url-parse` comes close, but the class is its default export, not a name.
// So this little adapter just re-exports it under the name `URL`.

import * as urlparse from 'url-parse';

export let URL = urlparse;
