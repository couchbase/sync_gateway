#!/bin/bash

# Run Sync Gateway test suite
#
# - Unit tests against walrus
# - Unit tests against Couchbase Server (aka tntegration tests)
#    - With XATTRS 
#    - Without XATTRS (sync meta legacy mode)
# - Code Coverage Report (merged)
#
# Unit tests against walrus
# -------------------------
#
# ./test.sh 
#
# Unit tests against Couchbase Server
# -----------------------------------
#
# ./test.sh -d http://localhost:8091 [-x] 
#
# NOTE: If the -d flag is specified, it will skip the Walrus tests.  
# NOTE: By default it will use sync meta legacy mode.  Optionally specify -x to enable XATTR mode
#
# Code Coverage Report
# --------------------
#
# ./test.sh [...] -c
#
# NOTE: works in conjunction with other arguments.  If this is enabled, it will run tests
#       with AlekSi/gocoverutil as a wrapper util
#
# Passing additional arguments to go tools
# ----------------------------------------
#
# ./test.sh [...] -- -run MyTest


set -e 

# --------------------------------- Constants/Variables -----------------------------------------

SG_PACKAGES="github.com/couchbase/sync_gateway/..."
SG_ACCEL_DIR="godeps/src/github.com/couchbaselabs/sync-gateway-accel"
SG_ACCEL_PACKAGES="github.com/couchbaselabs/sync-gateway-accel/..."
COUCHBASE_BACKING_STORE="Couchbase"
TEST_COVERAGE_REPORT=0
SG_COVERAGE_REPORT_DEST="coverage_sync_gateway.html"
SG_ACCEL_COVERAGE_REPORT_DEST="coverage_sg_accel.html"
SG_TEST_BACKING_STORE="Walrus"
SG_TEST_USE_XATTRS="false"
SHIFT_COUNTER=0
# --------------------------------- Helper Functions -----------------------------------------


verifyCoverageUtil() {
  # Make sure gocoverutil is in path
  path_to_gocoverutil=$(which gocoverutil)
  if [ -x "$path_to_gocoverutil" ] ; then
      echo "Using gocoverutil: $path_to_gocoverutil"
  else
      echo "Please install gocoverutil by running 'go get -u github.com/AlekSi/gocoverutil'"
      exit 1
  fi
}

# Parse the options and save into variables
parseOptions() {
    
    while getopts "d:cx" opt; do
	case $opt in
	    c)
		# If the -c option is set, run tests with gocoverutil.
		TEST_COVERAGE_REPORT=1
    # Make sure the gocoverutil is installed first
    verifyCoverageUtil
    # Remove args so they don't get propagated
    SHIFT_COUNTER=$((SHIFT_COUNTER+1))
    echo "Enabling test coverage report generation"
		;;
	    d)
    export SG_TEST_BACKING_STORE=$COUCHBASE_BACKING_STORE
		export SG_TEST_COUCHBASE_SERVER_URL=$OPTARG
		echo "Using Couchbase Server URL: $SG_TEST_COUCHBASE_SERVER_URL"
    # Remove args so they don't get propagated
    SHIFT_COUNTER=$((SHIFT_COUNTER+2))
		;;
	    x)
		export SG_TEST_USE_XATTRS="true"
    echo "Enabling XATTR mode: $SG_TEST_USE_XATTRS"
    # Remove args so they don't get propagated
    SHIFT_COUNTER=$((SHIFT_COUNTER+1))
		;;
	    h)
		echo "See comments in the top of this shell script for usage examples"
		exit 0
		;;
	    \?)
		echo "Invalid option: -$OPTARG.  Aborting" >&2
    exit 1
		;;
	esac
    done

}

setGoPath() {
  if [ -d "godeps" ]; then
    export GOPATH=`pwd`/godeps
    echo "Using GOPATH: $GOPATH"
  else
    echo "Did not find godeps subdirectory."
    exit 1
  fi
}

runWalrusTestsIfEnabled() {

  if [ "$SG_TEST_BACKING_STORE" == "$COUCHBASE_BACKING_STORE" ]; then
    echo "Integration tests enabled, skipping walrus tests"
    return
  fi

  if [ "$SG_TEST_USE_XATTRS" == "true" ]; then
    echo "Walrus tests cannot be run with SG_TEST_USE_XATTRS=true"
    return
  fi

  if [ $TEST_COVERAGE_REPORT -eq 1 ]; then
    runWalrusOrIntegrationTestsWithCoverage "$@"
  else
    runWalrusTests "$@"
  fi

}

runWalrusTests() {
  echo "Running Sync Gateway unit tests against $SG_TEST_BACKING_STORE"
  go test -v "$@" $SG_PACKAGES

  if [ -d $SG_ACCEL_DIR ]; then
      echo "Running Sync Gateway Accel unit tests against $SG_TEST_BACKING_STORE"
      go test -v "$@" $SG_ACCEL_PACKAGES
  fi
}

runWalrusOrIntegrationTestsWithCoverage() {

  echo "Running Sync Gateway unit tests against $SG_TEST_BACKING_STORE with test coverage report"
  gocoverutil -coverprofile=cover_sg.out test "$@" -v -covermode=count $SG_PACKAGES
  go tool cover -html=cover_sg.out -o $SG_COVERAGE_REPORT_DEST
  rm cover_sg.out
  echo "Sync Gateway test coverage report saved to: $SG_COVERAGE_REPORT_DEST"

  if [ -d $SG_ACCEL_DIR ]; then
      echo "Running Sync Gateway Accel unit tests against $SG_TEST_BACKING_STORE with test coverage report"
      gocoverutil -coverprofile=cover_sga.out test "$@" -v -covermode=count $SG_ACCEL_PACKAGES
      go tool cover -html=cover_sga.out -o $SG_ACCEL_COVERAGE_REPORT_DEST
      rm cover_sga.out 
      echo "Sync Gateway Accel test coverage report saved to: $SG_ACCEL_COVERAGE_REPORT_DEST"
  fi

}

runIntegrationTestsIfEnabled() {

  if [ "$SG_TEST_BACKING_STORE" != "$COUCHBASE_BACKING_STORE" ]; then
    echo "Integration tests not enabled, skipping"
    return
  fi

  if [ $TEST_COVERAGE_REPORT -eq 1 ]; then
    # This will probably fail due to known issues with the integration suite
    # https://github.com/couchbase/sync_gateway/issues/2612.
    # Using runIntegrationTests() here is difficult due to all the coverage report merging required.
    runWalrusOrIntegrationTestsWithCoverage "$@"
  else
    runIntegrationTests "$@"
  fi

}

runIntegrationTests() {

  # This is a test suite to run the SG and SG Accel unit tests against a live Couchbase Server
  # bucket (as opposed to walrus).  It works around the "interference" issues that seem to happen
  # when trying to run the full test suite (eg, running test.sh) as follows (maybe due to residual objects in Sync Gateway
  # memory like timer callbacks).  See https://github.com/couchbase/sync_gateway/issues/2612.
  #
  # Regarding maintenance, this might be a temporary script until the interference issues mentioned above
  # are solved.  However, if it does become a permanent script, then it should probably be upgraded to
  # leverage a tool like https://github.com/ungerik/pkgreflect to discover the list of tests.

  echo "Running Sync Gateway unit tests against $SG_TEST_BACKING_STORE"

  # Run Sync Gateway integration Tests
  for i in "${arr[@]}"
  do
      go test -v "$@" -run ^"$i"$ $SG_PACKAGES
  done

  # Run Sync Gateway Accel integration tests as long as directory is present
  if [ -d $SG_ACCEL_DIR ]; then  
    echo "Running Sync Gateway accel unit tests against $SG_TEST_BACKING_STORE"
 
    for i in "${arr_sgaccel[@]}"
    do
        go test -v "$@" -run ^"$i"$ $SG_ACCEL_PACKAGES
    done
  fi

}


# --------------------------------- Sync Gateway Integration tests -----------------------------------------

declare -a arr=(
    "TestChannelCacheSize"
    "TestChangesAfterChannelAdded"
    "TestDocDeletionFromChannelCoalescedRemoved"
    "TestDocDeletionFromChannelCoalesced"
    "TestDuplicateDocID"
    "TestLateArrivingSequence"
    "TestLateSequenceAsFirst"
    "TestDuplicateLateArrivingSequence"
    "TestChannelIndexBulkGet10"
    "TestChannelIndexSimpleReadSingle"
    "TestChannelIndexSimpleReadBulk"
    "TestChannelIndexPartitionReadSingle"
    "TestChannelIndexPartitionReadBulk"
    "TestVbucket"
    "TestChannelVbucketMappings"
    "TestDatabase"
    "TestGetDeleted"
    "TestAllDocs"
    "TestUpdatePrincipal"
    "TestConflicts"
    "TestSyncFnOnPush"
    "TestInvalidChannel"
    "TestAccessFunctionValidation"
    "TestAccessFunction"
    "TestDocIDs"
    "TestUpdateDesignDoc"
    "TestImport"
    "TestPostWithExistingId"
    "TestPutWithUserSpecialProperty"
    "TestWithNullPropertyKey"
    "TestPostWithUserSpecialProperty"
    "TestIncrRetrySuccess"
    "TestIncrRetryUnsuccessful"
    "TestRecentSequenceHistory"
    "TestChannelView"
    "TestQueryAllDocs"
    "TestViewCustom"
    "TestParseXattr"
    "TestParseDocumentCas"
    "TestWebhookString"
    "TestSanitizedUrl"
    "TestDocumentChangeEvent"
    "TestDBStateChangeEvent"
    "TestSlowExecutionProcessing"
    "TestCustomHandler"
    "TestUnhandledEvent"
    "TestWebhookBasic"
    "TestWebhookOldDoc"
    "TestWebhookTimeout"
    "TestUnavailableWebhook"
    "TestIndexBlockCreation"
    "TestIndexBlockStorage"
    "TestDenseBlockSingleDoc"
    "TestDenseBlockMultipleInserts"
    "TestDenseBlockMultipleUpdates"
    "TestDenseBlockRemovalByKey"
    "TestDenseBlockRollbackTo"
    "TestDenseBlockConcurrentUpdates"
    "TestDenseBlockIterator"
    "TestDenseBlockList"
    "TestDenseBlockListBadCas"
    "TestDenseBlockListConcurrentInit"
    "TestDenseBlockListRotate"
    "TestCalculateChangedPartitions"
    "TestRevisionCache"
    "TestLoaderFunction"
    "TestRevTreeUnmarshalOldFormat"
    "TestRevTreeUnmarshal"
    "TestRevTreeMarshal"
    "TestRevTreeAccess"
    "TestRevTreeParentAccess"
    "TestRevTreeGetHistory"
    "TestRevTreeGetLeaves"
    "TestRevTreeForEachLeaf"
    "TestRevTreeAddRevision"
    "TestRevTreeCompareRevIDs"
    "TestRevTreeIsLeaf"
    "TestRevTreeWinningRev"
    "TestPruneRevisions"
    "TestParseRevisions"
    "TestEncodeRevisions"
    "TestTrimEncodedRevisionsToAncestor"
    "TestHashCalculation"
    "TestHashStorage"
    "TestConcurrentHashStorage"
    "TestParseSequenceID"
    "TestMarshalSequenceID"
    "TestSequenceIDUnmarshalJSON"
    "TestMarshalTriggeredSequenceID"
    "TestCompareSequenceIDs"
    "TestShadowerPull"
    "TestShadowerPullWithNotifications"
    "TestShadowerPush"
    "TestShadowerPushEchoCancellation"
    "TestShadowerPullRevisionWithMissingParentRev"
    "TestShadowerPattern"
    "TestUserAPI"
    "TestUserPasswordValidation"
    "TestUserAllowEmptyPassword"
    "TestUserDeleteDuringChangesWithAccess"
    "TestRoleAPI"
    "TestGuestUser"
    "TestSessionTtlGreaterThan30Days"
    "TestSessionExtension"
    "TestSessionAPI"
    "TestFlush"
    "TestDBOfflineSingle"
    "TestDBOfflineConcurrent"
    "TestStartDBOffline"
    "TestDBOffline503Response"
    "TestDBOfflinePutDbConfig"
    "TestDBOfflinePostResync"
    "TestDBOnlineSingle"
    "TestDBOnlineConcurrent"
    "TestSingleDBOnlineWithDelay"
    "TestDBOnlineWithDelayAndImmediate"
    "TestDBOnlineWithTwoDelays"
    "TestPurgeWithBadJsonPayload"
    "TestPurgeWithNonArrayRevisionList"
    "TestPurgeWithEmptyRevisionList"
    "TestPurgeWithGreaterThanOneRevision"
    "TestPurgeWithNonStarRevision"
    "TestPurgeWithStarRevision"
    "TestPurgeWithMultipleValidDocs"
    "TestPurgeWithSomeInvalidDocs"
    "TestReplicateErrorConditions"
    "TestDocumentChangeReplicate"
    "TestRoot"
    "TestDocLifecycle"
    "TestDocEtag"
    "TestDocAttachment"
    "TestDocAttachmentOnRemovedRev"
    "TestDocumentUpdateWithNullBody"
    "TestFunkyDocIDs"
    "TestFunkyDocAndAttachmentIDs"
    "TestCORSOrigin"
    "TestCORSLoginOriginOnSessionPost"
    "TestCORSLoginOriginOnSessionPostNoCORSConfig"
    "TestNoCORSOriginOnSessionPost"
    "TestCORSLogoutOriginOnSessionDelete"
    "TestCORSLogoutOriginOnSessionDeleteNoCORSConfig"
    "TestNoCORSOriginOnSessionDelete"
    "TestManualAttachment"
    "TestManualAttachmentNewDoc"
    "TestBulkDocs"
    "TestBulkDocsEmptyDocs"
    "TestBulkDocsMalformedDocs"
    "TestBulkGetEmptyDocs"
    "TestBulkDocsChangeToAccess"
    "TestBulkDocsNoEdits"
    "TestRevsDiff"
    "TestOpenRevs"
    "TestLocalDocs"
    "TestResponseEncoding"
    "TestLogin"
    "TestReadChangesOptionsFromJSON"
    "TestAccessControl"
    "TestVbSeqAccessControl"
    "TestChannelAccessChanges"
    "TestAccessOnTombstone"
    "TestUserJoiningPopulatedChannel"
    "TestRoleAssignmentBeforeUserExists"
    "TestRoleAccessChanges"
    "TestAllDocsChannelsAfterChannelMove"
    "TestAttachmentsNoCrossTalk"
    "TestOldDocHandling"
    "TestStarAccess"
    "TestCreateTarget"
    "TestBasicAuthWithSessionCookie"
    "TestEventConfigValidationSuccess"
    "TestEventConfigValidationInvalid"
    "TestBulkGetRevPruning"
    "TestDocExpiry"
    "TestUnsupportedConfig"
    "TestChangesAccessNotifyInteger"
    "TestChangesNotifyChannelFilter"
    "TestDocDeletionFromChannel"
    "TestPostChangesInteger"
    "TestPostChangesUserTiming"
    "TestPostChangesSinceInteger"
    "TestPostChangesWithQueryString"
    "TestPostChangesChannelFilterInteger"
    "TestPostChangesAdminChannelGrantInteger"
    "TestChangesLoopingWhenLowSequence"
    "TestUnusedSequences"
    "TestChangesActiveOnlyInteger"
    "TestOneShotChangesWithExplicitDocIds"
    "TestMaxSnapshotsRingBuffer"
    "TestGetRestrictedIntQuery"
    "TestParseHTTPRangeHeader"
    "TestSanitizeURL"
    "TestVerifyHTTPSSupport",
    "TestChangesIncludeConflicts",
    "TestGetRemoved",
    "TestGetRemovedAndDeleted",
    "TestGetRemovedAsUser",
    "TestXattrSGTombstone")


# --------------------------------- SG Accel tests -----------------------------------------

declare -a arr_sgaccel=(
    "TestChannelWriterOnly"
    "TestChannelWriterAddSet"
    "TestChannelWriterAddSetMultiBlock"
    "TestChannelWriterClock"
    "TestPartitionStorage"
    "TestChannelStorageCorrectness_BitFlag"
    "TestChannelStorageCorrectness_Dense"
    "TestChannelStorage_Write_Ops_BitFlag"
    "TestChannelStorage_Write_Ops_Dense"
    "TestChannelStorage_Read_Ops_BitFlag"
    "TestChannelStorage_Read_Ops_Dense"
    "TestChangeIndexAddEntry"
    "TestChangeIndexGetChanges"
    "TestChangeIndexConcurrentWriters"
    "TestChangeIndexConcurrentWriterHandover"
    "TestChangeIndexAddSet"
    "TestDocDeletionFromChannel"
    "TestPostChangesClockBasic"
    "TestPostBlockListRotate"
    "TestPostChangesClockAdmin"
    "TestPostChangesSameVbucket"
    "TestPostChangesSinceClock"
    "TestPostChangesChannelFilterClock"
    "TestMultiChannelUserAndDocs"
    "TestDocDeduplication"
    "TestIndexChangesMultipleRevisions"
    "TestPostChangesAdminChannelGrantClock"
    "TestChangesLoopingWhenLowSequence"
    "TestChangesActiveOnlyClock"
    "TestChangesAccessNotifyClock"
    "TestChangesAccessWithLongpoll"
    "TestStorageReaderCache"
    "TestStorageReaderCacheUpdates"
    "TestStorageReaderCacheSingleDocUpdate"
    "TestIndexWriterRollback"
    "TestChangesBackfillOneshot"
    "TestChangesOneshotLimitSyncGrantMixedBackfill"
    "TestChangesOneshotLimitSyncGrantNoBackfill"
    "TestChangesOneshotLimitAdminGrant"
    "TestInterruptedBackfillWithWritesOnly"
    "TestInterruptedBackfillWithWritesAndHistory"
    "TestInterruptedBackfillWithWritesAndGrantVisibility"
    "TestChangesOneshotLimitRoleGrant"
    "TestChangesLongpollLimitSyncRoleChannelGrant"
    "TestChangesLongpollLimitSyncRoleGrant"
    "TestChangesOneshotLimitRoleAdminGrant"
)

# Parse the getopt options and set variables
parseOptions "$@"

# Remove (by shifting) all of the options and option args that
# were processed by parseOptions, so that the rest of the options can be propagated 
if [ $SHIFT_COUNTER -gt 0 ]; then
    shift $SHIFT_COUNTER
fi

# If there was a "--" given to signal the end of the options, 
# so that parseOptions doesn't try to process them, then remove it here.
if [ $# -gt 0 ]; then
    shift
fi

setGoPath 

# Run Walrus Tests If Enabled
runWalrusTestsIfEnabled "$@"

# Run Integration Tests If Enabled
runIntegrationTestsIfEnabled "$@"


