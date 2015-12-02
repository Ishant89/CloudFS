#!/bin/bash
#
# A script to test if the basic functions of the files 
# in CloudFS. Has to be run from the ./src/scripts/ 
# directory.
# 
TEST_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source $TEST_DIR/../../../scripts/paths.sh

CLOUDFS=$cloudfs_bin
FUSE=$fuse_mnt
SSD=$ssd_mnt
CLOUD="/tmp/s3"
CLOUDFSOPTS=""
SSDSIZE=""
THRESHOLD="16"
AVGSEGSIZE="4"
RABINWINDOWSIZE=""
CACHESIZE=""
NODEDUP="0"
NOCACHE="0"
LOG_DIR="/tmp/testrun-`date +"%Y-%m-%d-%H%M%S"`"
TESTDIR="$FUSE_MNT"
TEMPDIR="/tmp/cloudfstest"
STATFILE="$LOG_DIR/stats"
TARFILE="$TEST_DIR/big_test.tar.gz"

CACHEDIR="/home/student/mnt/ssd/.cache/"

source $SCRIPTS_DIR/functions.sh

#
# Execute battery of test cases.
# expects that the test files are in $TESTDIR
# and the reference files are in $TEMPDIR
# Creates the intermediate results in $LOG_DIR
#
function execute_part2_tests()
{

    #----
    # Testcases
    # assumes out test data does not have any hiddenfiles(.* files)
    # students should have all their metadata in hidden files/dirs
    echo ""
    echo "Executing test_2_2"
    rm -rf $CACHEDIR
    reinit_env
     
    untar $TARFILE $TESTDIR 
    untar $TARFILE $TEMPDIR

    PWDSAVE=$PWD
    cd $TEMPDIR && find .  \( ! -regex '.*/\..*' \) -type f -exec md5sum \{\} \; | sort -k2 > $LOG_DIR/md5sum.out.master
    collect_stats > $STATFILE.md5sum
    cd $TESTDIR && find .  \( ! -regex '.*/\..*' \) -type f -exec md5sum \{\} \; | sort -k2 > $LOG_DIR/md5sum.out
    collect_stats >> $STATFILE.md5sum
    cd $PWDSAVE

    echo -ne "Checking for file integrity : "
    diff $LOG_DIR/md5sum.out.master $LOG_DIR/md5sum.out
    print_result $?

    echo "Requests to cloud       : `get_cloud_requests $STATFILE.md5sum`"
    echo "Bytes read from cloud   : `get_cloud_read_bytes $STATFILE.md5sum`"
    echo "Capacity usage in cloud : `get_cloud_current_usage $STATFILE.md5sum`"

    echo "Cloud cost = `calculate_cloud_cost $STATFILE.md5sum`"

    du -s $TEMPDIR | awk '{print $1}' >> $STATFILE.tar_size 
    du -s $SSD_MNT | awk '{print $1}' >> $STATFILE.ssd_size
    echo "$(get_cloud_current_usage $STATFILE.md5sum)" >> $STATFILE.cloud_usage

    echo -e "\nCompare the disk usage to the total size of files copied to the disk..."
    nbytes_tar=$(<$STATFILE.tar_size) 
    nbytes_ssd=$(<$STATFILE.ssd_size)
    echo -ne "Comparison test(in KB)   "    
    test $nbytes_ssd -lt $nbytes_tar
    print_result $?

    echo -e "\nCompare the cloud usage to the total size of files copied to the disk..."
    nbytes_cloud=$(<$STATFILE.cloud_usage)
    k=1000
    #convert to kilobytes
    nbytes_cloud=$(($nbytes_cloud/$k))
    echo -ne "Comparison test(in KB)   "    
    test $nbytes_cloud -lt $nbytes_tar
    print_result $?

    echo ""
    #----
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
execute_part2_tests
#----
# test cleanup

echo -ne "File removal test (rm -rf)        "
rm -rf $FUSE_MNT/*
LF="$LOG_DIR/files-remaining-after-rm-rf.out"

ls $FUSE_MNT > $LF
find $SSD_MNT \( ! -regex '.*/\..*' \) -type f >> $LF
find $S3_DIR \( ! -regex '.*/\..*' \) -type f >> $LF
nfiles=`wc -l $LF|cut -d" " -f1`
print_result $nfiles

rm -rf $TEMPDIR
rm -rf $LOG_DIR

exit 0

