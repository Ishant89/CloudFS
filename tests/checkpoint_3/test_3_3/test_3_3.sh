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

#
# Execute battery of test cases.
# expects that the test files are in $TESTDIR
# and the reference files are in $TEMPDIR
# Creates the intermediate results in $LOGDIR
#
function execute_part3_tests()
{

   echo "Executing test_3_3"
   reinit_env

   # Copy largefile and smallfile_1
   cp $TEST_DIR/largefile $TESTDIR
   cp $TEST_DIR/largefile $TEMPDIR
   sleep 1

   cp $TEST_DIR/smallfile_1 $TESTDIR
   cp $TEST_DIR/smallfile_1 $TEMPDIR

   # create snapshot 1
   echo -ne "Checking for snapshot creation    "
   snapshot_num=$($SCRIPTS_DIR/snapshot $FUSE_MNT/.snapshot s)
   if [ $? -ne 0 ]; then
      print_result 1 
      exit
   else
      print_result 0
   fi

   rm $TESTDIR/largefile
   rm $TESTDIR/smallfile_1

   # Copy largefile and smallfile_2
   cp $TEST_DIR/bigfile $TESTDIR
   cp $TEST_DIR/smallfile_2 $TESTDIR
   sleep 1

   # restore a snapshot
   echo -ne "Checking for snapshot restore                     "
   $SCRIPTS_DIR/snapshot $FUSE_MNT/.snapshot r $snapshot_num
   if [ $? -ne 0 ]; then
      print_result 1 
      exit
   else
      print_result 0
   fi

   # Check for data integrity
   echo -ne "Checking for data integrity(multiple checks)       "
   diff $TEMPDIR/largefile $TESTDIR/largefile
   print_result $?
   diff $TEMPDIR/smallfile_1 $TESTDIR/smallfile_1
   print_result $?

   # Files that must not exist.
   if [ ! -f $TESTDIR/smallfile_2 ]; then
       print_result 0
   else
       print_result 1
   fi

   if [ ! -f $TESTDIR/bigfile ]; then
       print_result 0
   else
       print_result 1
   fi
}

#
# Main
#

process_args cloudfs --threshold $THRESHOLD --avg-seg-size $AVGSEGSIZE

#----
# test setup
rm -rf $TEMPDIR
mkdir -p $TEMPDIR
mkdir -p $LOG_DIR

#----
# tests
#run the actual tests
execute_part3_tests
#----

rm -rf $TEMPDIR
rm -rf $LOG_DIR

exit 0
