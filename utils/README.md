# Admin Interface for Couchbase Sync Gateway

To use this interface visit http://localhost:4985/_utils/ in your browser.

## What can it do?

The main goal is to support Sync Function debug and development. So you can edit your Sync Function code and see what it will do *before* you deploy it.

## Known Issues

Currently it tries to load the last 1000 changes into the brower's memory. If you have more than 1000 documents in your database it will only look at the 1000 most recent. In the future we will make this configurable.

## Developing / Contributing

Before you can work on this code, you need nodejs installed locally. Once you have that, run these commands.

	ch utils/
	npm install
	npm install -g browserify
	./modules.sh

You'll need to run `./modules.sh` every time you change the syncModel.js file. (Or if you decide to add a new NPM dependency)
