buildit:
	GOBIN="`pwd`/bin" ./go.sh install -v github.com/couchbase/sync_gateway
	@echo "Success! Output is bin/sync_gateway"
clean:
	rm -rf bin pkg vendor/pkg
buildclean: clean buildit
cleanbuild: clean buildit
test:
	@./test.sh
