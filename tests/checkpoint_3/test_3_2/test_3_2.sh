#!/bin/bash
#
# A script to test if the basic functions of the files 
# in CloudFS. Has to be run from the ./src/scripts/ 
# directory.
# 
TEST_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source $TEST_DIR/../../../scripts/paths.sh
source $SCRIPTS_DIR/functions.sh

THRESHOLD="64"
AVGSEGSIZE="4"
LOG_DIR="/tmp/testrun-`date +"%Y-%m-%d-%H%M%S"`"
TESTDIR=$FUSE_MNT
TEMPDIR="/tmp/cloudfstest"
STATFILE="$LOG_DIR/stats"
TARFILE="$TEST_DIR/big_test.tar.gz"

#
# Execute battery of test cases.
# expects that the test files are in $TESTDIR
# and the reference files are in $TEMPDIR
# Creates the intermediate results in $LOGDIR
#

function execute_part3_tests()
{

    echo "Executing test_3_2"
    reinit_env
   
    # create the test data in FUSE dir
    untar $TARFILE $TESTDIR 
    # create a reference copy on ext2
    untar $TARFILE $TEMPDIR

    echo -ne "Checking for snapshot creation    "
    snapshot_num=$($SCRIPTS_DIR/snapshot $FUSE_MNT/.snapshot s) 
    if [ $? -ne 0 ]; then
      print_result 1 
      exit
    else
      print_result 0
    fi

    # unmount and remount
    $SCRIPTS_DIR/cloudfs_controller.sh u 
    $SCRIPTS_DIR/umount_disks.sh
    $SCRIPTS_DIR/format_disks.sh
    $SCRIPTS_DIR/mount_disks.sh
    
    $SCRIPTS_DIR/cloudfs_controller.sh x
    # restore a snapshot
    echo -ne "Checking for snapshot restore     "
    $SCRIPTS_DIR/snapshot $FUSE_MNT/.snapshot r $snapshot_num
    if [ $? -ne 0 ]; then
      print_result 1 
      exit
    else
      print_result 0
    fi

    echo -ne "Checking for data integrity       "
   	
    cd $TEMPDIR && ls -lR --time-style=+|grep -v '^total' > $LOG_DIR/expected.txt
    cd $TESTDIR && ls -lR --time-style=+|grep -v '^total' > $LOG_DIR/real.txt

    diff $LOG_DIR/expected.txt $LOG_DIR/real.txt
    print_result $?
}

# Main
#process_args cloudfs --ssd-path $SSD_MNT_ --fuse-path $FUSE_MNT_ --threshold $THRESHOLD --avg-seg-size $AVGSEGSIZE
process_args cloudfs --threshold $THRESHOLD --avg-seg-size $AVGSEGSIZE
#----

rm -rf $TEMPDIR
mkdir -p $TEMPDIR
mkdir -p $LOG_DIR

#run the actual tests
execute_part3_tests

#----
# test cleanup
rm -rf $TEMPDIR
rm -rf $LOG_DIR

exit 0
