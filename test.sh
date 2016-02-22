
## Go Tests
echo "Testing code with 'go test' ..."
GOPATH=`pwd`/godeps go test "$@" github.com/couchbase/sync_gateway/...

