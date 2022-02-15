#!/usr/bin/env bash

# Uncomment these if running locally
export COUCHBASE_SERVER_ADDR=127.0.0.1
export WORKSPACE=.
export TARGET_PACKAGE="..."
export RUN_COUNT=1
export XATTRS=true
export GSI=false
export TARGET_TEST=ALL
export SG_EDITION=EE
export COUCHBASE_SERVER_PROTOCOL=couchbase
export TLS_SKIP_VERIFY=true
export SG_TEST_BUCKET_POOL_SIZE=3
export PACKAGE_TIMEOUT=120m
export DETECT_RACES=false
export TEST_DEBUG=true
export SG_TEST_PROFILE_FREQUENCY=""

# Abort on errors
set -e

# Output all executed shell commands
set -x

# Check that Couchbase Server is reachable before we actually start
curl --fail http://Administrator:password@$COUCHBASE_SERVER_ADDR:8091/pools/default

# ANSI color sequences break XML report parsing, so can't enable
#export SG_COLOR="true"

# forces go to get private modules via ssh
git config --global url."git@github.com:".insteadOf "https://github.com/"



# Set up Go paths and check install
export GO111MODULE=auto

if [ -z $(echo :$PATH: | grep ":$(go env GOPATH)/bin:") ]; then
  export PATH=$PATH:$(go env GOPATH)/bin
fi
echo "PATH: $PATH"

go version
go env



# verbose tests
GO_TEST_FLAGS="-v"

# Should we run the tests with -race?
if [ "$DETECT_RACES" == "true" ]; then
    GO_TEST_FLAGS="$GO_TEST_FLAGS -race"
fi

# Should we run all tests, or be more specific?
if [ "$TARGET_TEST" != "ALL" ]; then
	GO_TEST_FLAGS="$GO_TEST_FLAGS -run $TARGET_TEST"
fi

if [ "$FAIL_FAST" == "true" ]; then
	GO_TEST_FLAGS="$GO_TEST_FLAGS -failfast"
fi

if [ "${PACKAGE_TIMEOUT}" != "" ]; then
	GO_TEST_FLAGS="$GO_TEST_FLAGS -timeout=$PACKAGE_TIMEOUT"
else
	echo "    Defaulting package timeout to 20m"
	GO_TEST_FLAGS="$GO_TEST_FLAGS -timeout=20m"
fi

## Test debug
if [ "$TEST_DEBUG" == "true" ]; then
    export SG_TEST_LOG_LEVEL="debug"
    export SG_TEST_BUCKET_POOL_DEBUG="true"
fi



# Run EE/CE walrus tests first (for aggregate coverage purposes)
# EE
go test -coverprofile=cover_unit_ee.out -tags cb_sg_enterprise $GO_TEST_FLAGS ./$TARGET_PACKAGE >verbose_unit_ee.out.raw 2>&1 | true
# CE
go test -coverprofile=cover_unit_ce.out $GO_TEST_FLAGS ./$TARGET_PACKAGE >verbose_unit_ce.out.raw 2>&1 | true



# Export SG integration test params
export SG_TEST_USE_XATTRS="$XATTRS"
export SG_TEST_USE_GSI="$GSI"
export SG_TEST_COUCHBASE_SERVER_URL="$COUCHBASE_SERVER_PROTOCOL://$COUCHBASE_SERVER_ADDR" # Localhost relative to the Jenkins node
export SG_TEST_TLS_SKIP_VERIFY="$TLS_SKIP_VERIFY"
export SG_TEST_BACKING_STORE="Couchbase"

if [ "$SG_EDITION" == "EE" ]; then
	GO_TEST_FLAGS="$GO_TEST_FLAGS -tags cb_sg_enterprise"
fi

# Should we get code coverage reports?
#GO_TEST_FLAGS="$GO_TEST_FLAGS -coverprofile=cover_int.out -coverpkg=github.com/couchbase/sync_gateway/..."
GO_TEST_FLAGS="$GO_TEST_FLAGS -coverprofile=cover_int.out"

# Set run count
GO_TEST_FLAGS="$GO_TEST_FLAGS -count=$RUN_COUNT"

# Set up cluster for integration tests
export SG_TEST_BACKING_STORE_RECREATE="false"
#./test-integration-init.sh



## Bucket pooling settings
#export SG_TEST_BUCKET_POOL_DEBUG="true"
export SG_TEST_BUCKET_POOL_SIZE="$SG_TEST_BUCKET_POOL_SIZE"
export SG_TEST_PROFILE_FREQUENCY="$SG_TEST_PROFILE_FREQUENCY"

# SG vars summary
(set | grep "SG_")

# Now finally run the integration tests (using the exit code whilst still piping into tee)
go test $GO_TEST_FLAGS -p 1 ./$TARGET_PACKAGE 2>&1 | tee verbose_int.out.raw
if [ "${PIPESTATUS[0]}" -ne "0" ]; then
  # the go test command failed, but we want to continue enough to grab test outputs/reports and then fail at the end of the job
  echo "go test failed! Will fail job after grabbing test reports"
  TESTFAILED=true
fi



# Strip non-printable characters
LC_CTYPE=C tr -dc [:print:][:space:] < verbose_unit_ee.out.raw > verbose_unit_ee.out
LC_CTYPE=C tr -dc [:print:][:space:] < verbose_unit_ce.out.raw > verbose_unit_ce.out
LC_CTYPE=C tr -dc [:print:][:space:] < verbose_int.out.raw > verbose_int.out



# Generate xunit test report that can be parsed by the Jenkins JUnit Plugin
mkdir -p reports
go get -v -u github.com/tebeka/go2xunit
go2xunit -suite-name-prefix="UNIT-EE-" -input verbose_unit_ee.out -output reports/test-unit-ee.xml
go2xunit -suite-name-prefix="UNIT-CE-" -input verbose_unit_ce.out -output reports/test-unit-ce.xml
go2xunit -suite-name-prefix="INT-" -input verbose_int.out -output reports/test-int.xml



# More coverage reporting stuff
	
# print coverage in console
go tool cover -func=cover_int.out | awk 'END{print "Total SG Integration Coverage: " $3}'

# Generate Go HTML coverage report
go tool cover -html=cover_unit_ee.out -o reports/coverage-unit-ee.html
go tool cover -html=cover_unit_ce.out -o reports/coverage-unit-ce.html
go tool cover -html=cover_int.out -o reports/coverage-int.html
	
# Get the Jenkins coverage reporting tools
go get -v -u github.com/axw/gocov/...
go get -v -u github.com/AlekSi/gocov-xml

# Generate Cobertura XML report that can be parsed by the Jenkins Cobertura Plugin
gocov convert cover_unit_ee.out | gocov-xml > reports/coverage-unit-ee.xml
gocov convert cover_unit_ce.out | gocov-xml > reports/coverage-unit-ce.xml
gocov convert cover_int.out | gocov-xml > reports/coverage-int.xml


if [ "$TESTFAILED" = true ] ; then
  # Check if verbose_int.out contained `FAIL:`
  #  - if yes, we had a test failure, so mark as unstable
  #  - if no, we had a test SETUP failure, so mark as failed
  if grep -q 'FAIL:' verbose_int.out; then
  	# test failure found - unstable
    exit 50
  else
  	# no test failure - but still failed
  	exit 1
  fi
fi
