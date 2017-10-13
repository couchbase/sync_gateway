set -e

## Go Tests
echo "Testing code with 'go test' ..."

if [ -d "godeps" ]; then
  export GOPATH=`pwd`/godeps
fi

# Make sure gocoverutil is in path
path_to_gocoverutil=$(which gocoverutil)
if [ -x "$path_to_gocoverutil" ] ; then
    echo "Using gocoverutil: $path_to_gocoverutil"
else
    echo "Please install gocoverutil by running 'go get -u github.com/AlekSi/gocoverutil'"
fi

echo "Running Sync Gateway unit tests"
gocoverutil -coverprofile=cover_sg.out test -v "$@" -covermode=count github.com/couchbase/sync_gateway/...

echo "Generating Sync Gateway HTML coverage report to coverage_sync_gateway.html"
go tool cover -html=cover_sg.out -o coverage_sync_gateway.html

if [ -d godeps/src/github.com/couchbaselabs/sync-gateway-accel ]; then
    echo "Running Sync Gateway Accel unit tests"
    gocoverutil -coverprofile=cover_sga.out test -v "$@" -covermode=count github.com/couchbaselabs/sync-gateway-accel/...

    echo "Generating SG Accel HTML coverage report to coverage_sg_accel.html"
    go tool cover -html=cover_sga.out -o coverage_sg_accel.html
fi