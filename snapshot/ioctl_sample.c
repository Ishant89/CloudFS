/** 
 * @file snapshot_test.c
 * @brief This file calls the snpashots ioctls implemented by 
 * cloudfs.
 * @author rohanseh
 * @date 2015-03-22
 */

#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include "../snapshot/snapshot-api.h"

int main(int argc, char **argv)
{

 unsigned long timestamp;
    int fd = open("/home/student/mnt/fuse/snapshot",O_CREAT|O_RDWR,S_IRWXU | S_IRWXG | S_IRWXO);
    if (fd < 0) 
    {
        perror(argv[1]);
        return 1;
    }

    if (ioctl(fd, CLOUDFS_SNAPSHOT,&timestamp )) 
    {
	printf("ioctl\n");
	return 1;
    }
    return 0;
}
