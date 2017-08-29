
## Go Tests
echo "Testing code with 'go test' ..."

if [ -d "godeps" ]; then
  export GOPATH=`pwd`/godeps
fi

go test "$@" github.com/couchbase/sync_gateway/...

