// Apollo's library imports the URL class from Node's `url` package.
// Unfortunately there isn't a polyfill library that works exactly like this.
// The `url-parse` comes close, but the class is its default export, not a name.
// So this little adapter just re-exports it under the name `URL`.

import * as urlparse from 'url-parse';

export let URL = urlparse;
