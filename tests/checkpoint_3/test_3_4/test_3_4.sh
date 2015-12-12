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

   echo "Executing test_3_4"
   reinit_env

   # create the cloud file 
   cp $TEST_DIR/largefile $TESTDIR
   cp $TEST_DIR/largefile $TEMPDIR
   
   # create a small file
   cp $TEST_DIR/smallfile $TESTDIR
   cp $TEST_DIR/smallfile $TEMPDIR

   # create a snapshot
   echo -ne "Checking for snapshot creation    "
   snapshot_num=$($SCRIPTS_DIR/snapshot $FUSE_MNT/.snapshot s)
   if [ $? -ne 0 ]; then
      print_result 1 
      exit
   else
      print_result 0
   fi

   SNAPDIR="$TESTDIR/snapshot_$snapshot_num"

   # install the snapshot
   echo -ne "Checking for snapshot install    "
   $SCRIPTS_DIR/snapshot $FUSE_MNT/.snapshot i $snapshot_num
   if [ $? -ne 0 ]; then
      print_result 1 
      exit
   else
      print_result 0
   fi

   # Checking for successful read
   
   # snapshot path creation
   SNAPDIR="$TESTDIR/snapshot_$snapshot_num"
   echo -ne "Checking for data integrity        "
   diff $TESTDIR/smallfile $SNAPDIR/smallfile
   print_result $?
   
   echo -ne "Checking for data integrity  "
   diff $TESTDIR/largefile $SNAPDIR/largefile
   print_result $?

   # Checking for write failures to installed snapshots
   echo -ne "Checking for writes to installed files(check 1)    "
   echo "hello" >> $SNAPDIR/largefile
   if [ $? -eq 0 ]; then
      echo "Error: successful write to installed files"
      print_result 1
      exit
   else 
      print_result 0
  fi

   echo -ne "Checking for writes to installed files(check 2)    "
   echo "hello" >> $SNAPDIR/smallfile
   if [ $? -eq 0 ]; then
      echo "Error: successful write to installed files"
      print_result 1
      exit
   else 
      print_result 0
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
