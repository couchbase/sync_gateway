#!/bin/sh

# This script uses 'go get' to pull the latest revisions of all of the 3rd party Go packages
# the gateway imports (and their transitive dependencies.) This is good to do periodically
# because otherwise these packages stay rev-locked thanks to git submodules. After doing this,
# test that the gateway still builds and passes unit tests. Then commit the changes to the
# submodule revisions.

export GOPATH="`pwd`/vendor"

go get -u github.com/couchbaselabs/go-couchbase
go get -u github.com/couchbaselabs/go.assert
go get -u github.com/couchbaselabs/walrus
go get -u github.com/dchest/passwordhash
go get -u github.com/gorilla/mux
go get -u github.com/robertkrimen/otto
go get -u github.com/robertkrimen/otto/underscore
go get -u github.com/tleyden/fakehttp
