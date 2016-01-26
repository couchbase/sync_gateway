export OLD_GOPATH=${GOPATH}
export CWD=`pwd`
export GOPATH=${CWD}
export GOBIN=${CWD}/bin

#Comment out the following line to go faster.
# TODO: Remove this once we submodule.
#go get -u github.com/go-swagger/go-swagger/cmd/swagger

swagger generate spec -o ${CWD}/build/swagger.json -b github.com/couchbase/sync_gateway
#swagger generate spec -o ${CWD}/build/swagger-test.json -b github.com/go-swagger/go-swagger/fixtures/goparsing/classification/operations

export GOPATH=${OLD_GOPATH}