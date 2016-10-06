
## Go Tests
echo "Testing code with 'go test' ..."

if [ -d "godeps" ]; then
  export GOPATH=`pwd`/godeps
fi

echo "Running Sync Gateway unit tests"
go test -v "$@" github.com/couchbase/sync_gateway/...

if [ -d godeps/src/github.com/couchbaselabs/sync-gateway-accel ]; then
    # when I tried this, it was hanging for a long time for me
    echo "NOT Running Sync Gateway Accel unit tests -- are these working?"
    #go test -v "$@" github.com/couchbaselabs/sync-gateway-accel/...
fi
