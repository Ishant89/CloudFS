#!/bin/bash
#
# A script to test if the basic functions of the files
# in CloudFS. Has to be run from the src directory.
#
TEST_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source $TEST_DIR/../../../scripts/paths.sh

REFERENCE_DIR="/tmp/cloudfstest"
LOG_DIR="/tmp/testrun-`date +"%Y-%m-%d-%H%M%S"`"
STAT_FILE="$LOG_DIR/stats"
THRESHOLD="16"
AVGSEGSIZE="4"
TEST_FILE="largefile"
TEST_FILE_DUP="largefile.duplicated"

source $SCRIPTS_DIR/functions.sh

#
# Execute battery of test cases.
# expects that the test files are in $FUSE_MNT
# and the reference files are in $REFERENCE_DIR
# Creates the intermediate results in $LOG_DIR
#
process_args cloudfs --threshold $THRESHOLD --avg-seg-size $AVGSEGSIZE

# test setup
rm -rf $REFERENCE_DIR
mkdir -p $REFERENCE_DIR
mkdir -p $LOG_DIR

reinit_env

#----
# Testcases assumes that test data does not have any hidden files(.* files)
# Students should have all their metadata in hidden files/dirs
echo ""
echo "Executing test_2_5"
echo -e "Running cloudfs with dedup\n"

echo -e "Copy test file into fuse ..."
cp $TEST_DIR/$TEST_FILE $FUSE_MNT/$TEST_FILE
cp $TEST_DIR/$TEST_FILE $FUSE_MNT/$TEST_FILE_DUP
    
# restart the cloudfs to check for persistency 
echo "Restarting cloudfs ..."
sleep 10

$SCRIPTS_DIR/cloudfs_controller.sh x $CLOUDFSOPTS

rm $FUSE_MNT/$TEST_FILE

echo -ne "Testing reference count persistency: "
diff $TEST_DIR/$TEST_FILE $FUSE_MNT/$TEST_FILE_DUP > $LOG_DIR/ref-count-persistency.diff
print_result $?

#----
#destructive test : always do this test at the end!!
echo -ne "File removal test (rm -rf)        "
rm -rf $FUSE_MNT/*
LF="$LOG_DIR/files-remaining-after-rm-rf.out"

ls $FUSE_MNT > $LF
find $SSD_MNT \( ! -regex '.*/\..*' \) -type f >> $LF
find $S3_DIR \( ! -regex '.*/\..*' \) -type f >> $LF
nfiles=`wc -l $LF|cut -d" " -f1`
print_result $nfiles

# test cleanup
rm -rf $REFERENCE_DIR
rm -rf $LOG_DIR
exit 0
