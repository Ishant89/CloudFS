#define ALERT
//#define INFO
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <getopt.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <time.h>
#include <unistd.h>
#include "cloudapi.h"
#include "cloudfs.h"
#include "dedup.h"

#define UNUSED __attribute__((unused))

int list_bucket(const char *key, time_t modified_time, uint64_t size) {
	  fprintf(stdout, "%s %lu %llu\n", key, modified_time, size);
	    return 0;
}

int list_service(const char *bucketName) {
	  fprintf(stdout, "%s\n", bucketName);
	    return 0;  
}

static FILE *outfile;
int get_buffer(const char *buffer, int bufferLength) {
	  return fwrite(buffer, 1, bufferLength, outfile);  
}

static FILE *infile;
int put_buffer(char *buffer, int bufferLength) {
	  fprintf(stdout, "put_buffer %d \n", bufferLength);
	    return fread(buffer, 1, bufferLength, infile);
}

static struct cloudfs_state state_;
/* File open for logging */
FILE* logfile;

static int UNUSED cloudfs_error(char *error_str)
{
    int retval = -errno;

    // TODO:
    //
    // You may want to add your own logging/debugging functions for printing
    // error messages. For example:
    //
    // debug_msg("ERROR happened. %s\n", error_str, strerror(errno));
    //
    
    fprintf(stderr, "CloudFS Error: %s and err string:%s\n", error_str,strerror(errno));
    fprintf(logfile, "CloudFS Error: %s and err string:%s\n", error_str,strerror(errno));

    /* FUSE always returns -errno to caller (yes, it is negative errno!) */
    return retval;
}

/*
 * Initializes the FUSE file system (cloudfs) by checking if the mount points
 * are valid, and if all is well, it mounts the file system ready for usage.
 *
 */
void *cloudfs_init(struct fuse_conn_info *conn UNUSED)
{
  cloud_init(state_.hostname);
  cloud_print_error();

  printf("Create bucket\n");
  cloud_create_bucket("cloudfs");
  cloud_print_error();

  return NULL;
}

void build_hidden_file_path(char * path,char * modified_path)
{
	char * temp = path;
	char * temp1 = path;
	char  filename[MAX_PATH_LEN];
	while((temp1 = strchr(temp1,'/')))
	{
		temp = temp1;
		temp1 = temp1 + 1;
	}
	strcpy(filename,temp + 1);
	*(temp + 1) = 0;
	strcpy(modified_path,path);
	strcat(modified_path,".");
	strcat(modified_path,filename);
}
void build_cloud_file_name(char * path,char * modified_path)
{
	strcpy(modified_path,path);
	char * temp1 = modified_path;
	while((temp1 = strchr(temp1,'/')))
	{
		*temp1 = '+';
		temp1 = temp1 + 1;
	}
}

void  get_from_bucket(char * cloud_file_name,char * file_path)
{
	INF("\n%s:%s Entering,cld file:%s,file_path:%s\n",
			cloud_file_name,file_path);
	outfile = fopen(file_path, "ab+");
	if(outfile == NULL)
	{
		INF("%s:%sFile not founds:%s",__func__);
		return ;
	}
	cloud_get_object("cloudfs",cloud_file_name, get_buffer);
	fclose(outfile);
	cloud_print_error();
	INF("\n%s:%s Exiting,cld file:%s,file_path:%s\n",
			cloud_file_name,file_path);
}
void cloudfs_destroy(void *data UNUSED) {
  cloud_destroy();
}

//int cloudfs_getattr(const char *path UNUSED, struct stat *statbuf UNUSED)
//{
//  int retval = 0;

  // 
  // TODO:
  //
  // Implement this function to do whatever it is supposed to do!
  //

  //return retval;
//}
/* File operations */
static void get_full_ssd_path(char * full_path,char * path)
{
        INF("\n%s:%s Entering:%s\n",__func__);
	strcpy(full_path,state_.ssd_path);
	strncat(full_path,path,MAX_PATH_LEN- strlen(path));
	INF("%s:%s Exiting:%s\n",full_path);
}
/* File read */
int cloudfs_open(char *path, struct fuse_file_info *fi)
{
    INF("\n%s:%s Entering:%s\n",path);
    int retval = 0;
    int fd;
    int retval1 = 0;
    char file_path[MAX_PATH_LEN];

    char org_path[MAX_PATH_LEN] ;
    char hidden_file_path[MAX_PATH_LEN] ;

    char abs_hidden_file_path[MAX_PATH_LEN] ;
    char cloud_file_name[MAX_PATH_LEN] ;

    struct stat hidden_statbuf1;
    struct stat cloud_local_file_stat;

    strcpy(org_path,path);
    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);
   /* Get the cloud file name */

    build_cloud_file_name(path,cloud_file_name);
    INF("%s:%s Cloud file name is %s\n",cloud_file_name);
    
    if (strcmp(path,"/"))
    {
           build_hidden_file_path(org_path,hidden_file_path);
           INF("%s:%s hidden file path is:%s\n",hidden_file_path);
	  /* Check if hidden file exists */
          
     	 /* Get the lstat of the hidden file if it exists */
           get_full_ssd_path(abs_hidden_file_path,hidden_file_path);
           retval1 = lstat(abs_hidden_file_path, &hidden_statbuf1);
	   INF("%s:%s lstat for hidden file(%s) retval:%d\n",abs_hidden_file_path,retval1); 
	   if (retval1 == 0)
	   {
		INF("%s:%s file exists:%s\n",abs_hidden_file_path);
		/* Get the file from the bucket*/
		INF("%s:%s Getting the file from bucket:%s",cloud_file_name);

		get_from_bucket(cloud_file_name,file_path);

		/* Get the lstat of the block after getting from cloud*/

		retval = lstat(file_path,&cloud_local_file_stat);
		if (retval < 0)
		{
			retval = cloudfs_error("Unable to locate stats on file");
			return retval;
		}
                /* File read was successful */
		FILE * file = fopen(abs_hidden_file_path,"wb+");
		INF("%s:%s File in getattr\n:%p",file);
		if (file == NULL)
		{
			INF("%s:%s File in getattr is NULL\n:%p",file);
			retval = cloudfs_error("cloudfs_getattr read hidden file failed");
			return retval;
		}
	   	retval = fwrite((void*)&cloud_local_file_stat,
				sizeof(struct stat),1,file);
		INF("%s:%s read ret code is :%d\n",retcode);
		if (retval == 0) {
			retval = cloudfs_error("cloudfs_getattr read hidden file failed");
			return retval;
		}
		//closefd
		fclose(file);
	 }
    }
    retval = 0 ;
    fd = open(file_path, fi->flags);
    if (fd < 0)
        retval = cloudfs_error("already open");

    fi->fh = fd;
    INF("%s:%s Exiting fd:%lld\n",fi->fh);
    return retval;
}

/** Create a directory */
int cloudfs_mkdir(char *path, mode_t mode)
{
    INF("\n%s:%s Entering:%s\n",path);
    int retval= 0;
    char file_path[PATH_MAX];


    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);
    retval = mkdir(file_path, mode);
    if (retval < 0)
        retval = cloudfs_error("mkdir error");

    INF("%s:%s Entering:%d\n",retval);
    return retval;
}


int cloudfs_readdir(char *path UNUSED, void *buf, fuse_fill_dir_t fill_dir, off_t offset UNUSED,
               struct fuse_file_info *fi)
{

    INF("\n%s:%s Entering:%s\n",path);
    int retval = 0;
    DIR *dp;
    struct dirent *de;

    // once again, no need for fullpath -- but note that I need to cast fi->fh
    dp = (DIR *) (uintptr_t) fi->fh;

    // Every directory contains at least two entries: . and ..  If my
    // first call to the system readdir() returns NULL I've got an
    // error; near as I can tell, that's the only condition under
    // which I can get an error from readdir()
    de = readdir(dp);
    if (de == 0) {
        retval = cloudfs_error("cloudfs_readdir readdir");
        return retval;
    }

    // This will copy the entire directory into the buffer.  The loop exits
    // when either the system readdir() returns NULL, or filler()
    // returns something non-zero.  The first case just means I've
    // read the whole directory; the second means the buffer is full.
    do {
	if (strcmp(de->d_name,"lost+found"))
	{
//	char * tmp;
	char * tmp1 = de->d_name;
	INF("%s:%s name:%s\n",de->d_name);
	   if (*tmp1 == '.')
		continue;
		if (fill_dir(buf, tmp1, NULL, 0) != 0) {
		    return -ENOMEM;
		}
	}
    } while ((de = readdir(dp)) != NULL);


    INF("%s:%s Exiting:%s\n",path);
    return retval;
}
char * get_file_name(char * path)
{
	char * temp;
	char * temp1 = path;
	while((temp = strchr(path,'/')))
	{
		*temp = 0;
		temp1 = temp + 1;
	}
	return temp1;
}


int cloudfs_getattr(char *path UNUSED, struct stat *statbuf UNUSED)
{
    INF("\n%s:%s Entering:%s\n",path);
    int retval = 0;
    struct stat statbuf1;
    char file_path[MAX_PATH_LEN];
    char org_path[MAX_PATH_LEN] ;
    char hidden_file_path[MAX_PATH_LEN] ;
    char abs_hidden_file_path[MAX_PATH_LEN] ;
    strcpy(org_path,path);
    int retcode=0,retval1=0,closefd=0;
    int fd;
    // On FreeBSD, trying to do anything with the mountpoint ends up
    // opening it, and then using the FD for an fgetattr.  So in the
    // special case of a path of "/", I need to do a getattr on the
    // underlying root directory instead of doing the fgetattr().
    if (strcmp(path,"/"))
    {
	    build_hidden_file_path(org_path,hidden_file_path);
	    INF("%s:%s hidden file path is:%s\n",hidden_file_path);
    

	    /* Get the lstat of the hidden file if it exists */
	    get_full_ssd_path(abs_hidden_file_path,hidden_file_path);
	    retval1 = lstat(abs_hidden_file_path, &statbuf1);
	    INF("%s:%s full hidden file path is:%sretval:%d\n",abs_hidden_file_path,retval1);
	    if (retval1 == 0 )
	    {
		INF("%s:%s file exists:%s\n",abs_hidden_file_path);
		/*
		 * File read was successful */
		FILE * file = fopen(abs_hidden_file_path,"rb");
		INF("%s:%s File in getattr\n:%p",file);
		if (file == NULL)
		{
			INF("%s:%s File in getattr is NULL\n:%p",file);
			retcode = cloudfs_error("cloudfs_getattr read hidden file failed");
			return retcode;
		}
		//fd = open(abs_hidden_file_path,O_RDWR);    
		INF("%s:%s FD is :%d\n",fd);
		retcode = fread((void*)statbuf,sizeof(struct stat),1,file);
		INF("%s:%s read ret code is :%d\n",retcode);
		if (retcode == 0) 
			retcode = cloudfs_error("cloudfs_getattr read hidden file failed");
		//closefd = close(fd);
		closefd = fclose(file);
		INF("%s:%s close code is :%d\n",closefd);
    		INF("%s:%s Exiting:%s\n",path);
	        return retval;	
	    }
    }
    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);
    retval = lstat(file_path, statbuf);
    INF("%s:%s full path is :%s\n",file_path);
    if (retval < 0)
        retval = cloudfs_error("cloudfs_getattr fstat");
    INF("%s:%s Exiting:%s\n",path);
    return retval;
}

/** Get extended attributes */
int cloudfs_getxattr(char *path,char *name,char *value, size_t size)
{
    INF("\n%s:%s Entering:%s,%s,%s\n",path,name,value);
    int retval = 0;
    char file_path[MAX_PATH_LEN];

    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);
    retval = lgetxattr(file_path, name, value, size);
    if (retval < 0)
        retval = cloudfs_error("cloudfs_getxattr lgetxattr");
    else
        INF("%s:%s name= %s,value = \"%s\"\n", name,value);

    INF("%s:%s Exiting:%s,%s,%s\n",path,name,value);
    return retval;
}
/** Set extended attributes */
int cloudfs_setxattr(char *path,char *name,char *value, size_t size,int flags)
{
    INF("\n%s:%s Entering:%s,%s,%s\n",path,name,value);
    int retval = 0;
    char file_path[MAX_PATH_LEN];

    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);

    retval = lsetxattr(file_path, name, value, size,flags);
    if (retval < 0)
        retval = cloudfs_error("cloudfs_setxattr lsetxattr");

    INF("%s:%s Exiting:%s,%s,%s\n",path,name,value);
    return retval;
}

/* Make file node */
int cloudfs_mknod(char *path, mode_t mode, dev_t dev)
{
    int retval = 0;
    char file_path[PATH_MAX];

    INF("\n%s:%s Entering,path:%s,mode:%o,dev:%lld\n",path, mode, dev);

    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);
    // On Linux this could just be 'mknod(path, mode, rdev)' but this
    //  is more portable
    if (S_ISREG(mode)) {
        retval = open(file_path, O_CREAT | O_EXCL | O_WRONLY, mode);
        if (retval < 0)
            retval = cloudfs_error("cloudfs_mknod open");
        else {
            retval = close(retval);
            if (retval < 0)
                retval = cloudfs_error("cloudfs_mknod close");
        }
    } else
        if (S_ISFIFO(mode)) {
            retval = mkfifo(file_path, mode);
            if (retval < 0)
                retval = cloudfs_error("cloudfs_mknod mkfifo");
        } else {
            retval = mknod(file_path, mode, dev);
            if (retval < 0)
                retval = cloudfs_error("cloudfs_mknod mknod");
        }

    INF("%s:%s Entering,path:%s,mode:%o,dev:%lld\n",path, mode, dev);
    return retval;
}

/*Read */
int cloudfs_read(char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    int retval = 0;

    INF("\n%s:%sEntering(path=\"%s\", buf=%p, size=%d, offset=%lld, fi=%p)\n",
            path, buf, size, offset, fi);

    retval = pread(fi->fh, buf, size, offset);
    if (retval < 0)
        retval = cloudfs_error("cloudfs_read read");
    
    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}

/* Write */

int cloudfs_write(char *path UNUSED, char *buf, size_t size, off_t offset,
             struct fuse_file_info *fi)
{
    int retval = 0;

    INF("\n%s:%s Entering(path=\"%s\", buf=%p, size=%d, offset=%lld, fi=%p)\n",
            path, buf, size, offset, fi
            );

    retval = pwrite(fi->fh, buf, size, offset);
    if (retval < 0)
        retval = cloudfs_error("cloudfs_write pwrite");

    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}
void  write_to_bucket(char * cloud_file_name,uint64_t size,char * file_path)
{
	INF("\n%s:%s Entering,cld file:%s,file_path:%s\n",
			cloud_file_name,file_path);
	infile = fopen(file_path, "rb");
	if(infile == NULL)
	{
		INF("%s:%sFile not founds:%s",__func__);
		return ;
	}
	cloud_put_object("cloudfs",cloud_file_name, size, put_buffer);
	fclose(infile);
	cloud_print_error();
	INF("\n%s:%s Exiting,cld file:%s,file_path:%s\n",
			cloud_file_name,file_path);
}

void remove_bucket(char * cloud_file_name)
{
	INF("\n%s:%s Entering,cld file:%s\n",cloud_file_name);
	cloud_delete_object("cloudfs", cloud_file_name);
	cloud_print_error();

	INF("\n%s:%s Exiting ,cld file:%s\n",cloud_file_name);

}
/* Release the file */
int cloudfs_release(char *path, struct fuse_file_info *fi)
{
    ALT("%s:%s Enter fi->fh :%lld\n",fi->fh); 
    int retval = 0;
    char cloud_file_name[MAX_PATH_LEN];
    struct stat hidden_statbuf1;
    char file_path[MAX_PATH_LEN];
    char org_path[MAX_PATH_LEN] ;
    char hidden_file_path[MAX_PATH_LEN] ;
    char abs_hidden_file_path[MAX_PATH_LEN] ;
    int retcode=0,retval1=0,closefd=0;
    int fd;

    INF("\n%s:%s Entering(path=\"%s\", fi=%p)\n",
          path, fi);
    int errorcode = 0;
    strcpy(org_path,path);
    /* Get the current size of the open file */
    struct stat current_file;

/* Get the full path of the file */
     get_full_ssd_path(file_path,path);
    
    errorcode = lstat(file_path,&current_file);
     ALT("%s:%s After fstat fi->fh :%lld\n",fi->fh); 
    if (errorcode < 0)
    {
	errorcode = cloudfs_error("Release unable to fetch details about current file");
	return errorcode;
    }
/* Get the file size and at & mod times */
    uint64_t size_curr_file = current_file.st_size;
    INF("%s:%s Size of curr file is %lld\n",size_curr_file);
    time_t curr_file_access_time = current_file.st_atime;   
    time_t curr_file_mod_time = current_file.st_mtime; 

    INF("%s:%s curr file,at:%ld,mt:%ld\n",curr_file_access_time,curr_file_mod_time);
 /* declare hidden file parameters */
//    time_t hidden_file_access_time; 
    time_t hidden_file_mod_time; 
    struct stat cloud_file_stat;

     
   /* Get the cloud file name */
	build_cloud_file_name(path,cloud_file_name);
	INF("%s:%s Cloud file name is %s\n",cloud_file_name);
    if (strcmp(path,"/"))
    {
           build_hidden_file_path(org_path,hidden_file_path);
           INF("%s:%s hidden file path is:%s\n",hidden_file_path);
	  /* Check if hidden file exists */
          
     	 /* Get the lstat of the hidden file if it exists */
           get_full_ssd_path(abs_hidden_file_path,hidden_file_path);
           retval1 = lstat(abs_hidden_file_path, &hidden_statbuf1);
	   INF("%s:%s lstat for hidden file(%s) retval:%d\n",abs_hidden_file_path,retval1); 
	   if (retval1 == 0)
	   {
		   /*Hidden file exists */
		   /* Get the attributes from the hidden file*/
		INF("%s:%s file exists:%s\n",abs_hidden_file_path);
                /* File read was successful */
		FILE * file = fopen(abs_hidden_file_path,"rb");
		INF("%s:%s File in getattr\n:%p",file);
		if (file == NULL)
		{
			INF("%s:%s File in getattr is NULL\n:%p",file);
			retcode = cloudfs_error("cloudfs_getattr read hidden file failed");
			return retcode;
		}
		//fd = open(abs_hidden_file_path,O_RDWR);    
		//INF("%s:%s FD is :%d\n",fd);
		retcode = fread((void*)&cloud_file_stat,sizeof(struct stat),1,file);
		INF("%s:%s read ret code is :%d\n",retcode);
		if (retcode == 0) {
			retcode = cloudfs_error("cloudfs_getattr read hidden file failed");
			return retcode;
		}
		//closefd = close(fd);
		closefd = fclose(file);

                /*fd = open(abs_hidden_file_path,O_RDWR);
                INF("%s:%s FD is :%d\n",fd);
                retcode = pread(fd,&cloud_file_stat,sizeof(struct stat),0);
                INF("%s:%s read ret code is :%d\n",retcode);
                if (retcode < 0)
                        cloudfs_error("cloudfs_release read hidden file failed");*/

		/* Update the fields */
 		//hidden_file_access_time = cloud_file_stat.st_atime;
 		hidden_file_mod_time = cloud_file_stat.st_mtime;
                //closefd = close(fd);
                INF("%s:%s close code is :%d\n",closefd);
		/* Check if the file size is > threshold*/
		if (size_curr_file >= (unsigned int)state_.threshold)
		{
			/*Check the modification time */
			if(curr_file_mod_time > hidden_file_mod_time)
			{
				/* Write to the cloud */
				INF("%s:%s Writing to the cloud:%s\n",
						cloud_file_name);
				write_to_bucket(cloud_file_name,size_curr_file,
						file_path);
			}
			fd = open(abs_hidden_file_path,O_RDWR);
			INF("%s:%s FD for writing hidden file is :%d\n",fd);
			
			retcode = pwrite(fd,&current_file, sizeof(struct stat)
					, 0);
			if (retcode < 0)
			{
				retcode = cloudfs_error("cloudfs_close pwrite");
				return retcode;
			}

			closefd = close(fd);
			INF("%s:%s close code is :%d\n",closefd);
			
			/* Truncating the file */
			retcode = truncate(file_path, 0);
			if (retcode < 0)
			{
				 retcode = cloudfs_error("cloudfs_truncate truncate");
				 return retcode;
			}

			INF("%s:%s Truncating the file:%d\n",retcode);

			struct utimbuf ubuf;
			ubuf.actime = curr_file_access_time;
		   	ubuf.modtime = curr_file_mod_time;
			retcode = utime(file_path, &ubuf);
			if (retcode < 0)
			{
				retcode = cloudfs_error("cloudfs_utime utime");
				return retcode;
			}

    			retval = close(fi->fh);
			INF("%s:%s Exiting:%d\n",retcode);
			return retcode;

		}
		if (size_curr_file <= (unsigned int)state_.threshold)
		{
			/* Remove the cloud file */
			INF("%s:%s Removing the bucket:%s",cloud_file_name);

			remove_bucket(cloud_file_name);
			/* Remove the hidden file */
			INF("%s:%s Removing the hidden:%s",abs_hidden_file_path);
			errorcode = unlink(abs_hidden_file_path);
			if (errorcode< 0)
			{
				errorcode = cloudfs_error("cloudfs_unlink unlink");
				return errorcode;
			}

    			
			retval = close(fi->fh);
			INF("%s:%s Exiting:%d\n",retval);
			return errorcode;
		}

	   } else 
	   {
		   if (size_curr_file > (unsigned int)state_.threshold)
		   {
			/* File read was successful */
			/* Write to the cloud */
			INF("%s:%s Writing to the cloud:%s\n",
					cloud_file_name);
			write_to_bucket(cloud_file_name,size_curr_file,file_path);
		   	
			fd = open(abs_hidden_file_path,O_CREAT|O_RDWR,
					S_IRWXU | S_IRWXG | S_IRWXO);
			INF("%s:%s FD for writing hidden file is :%d\n",fd);
			
			retcode = pwrite(fd,&current_file, sizeof(struct stat)
					, 0);
			if (retcode < 0)
			{
				retcode = cloudfs_error("cloudfs_close pwrite");
				return retcode;
			}

			closefd = close(fd);
			INF("%s:%s close code is :%d\n",closefd);
			
			/* Truncating the file */
			retcode = truncate(file_path, 0);
			if (retcode < 0)
			{
				 retcode = cloudfs_error("cloudfs_truncate truncate");
				 return retcode;
			}

			INF("%s:%s Truncating the file:%d\n",retcode);

			struct utimbuf ubuf;
			ubuf.actime = curr_file_access_time;
		   	ubuf.modtime = curr_file_mod_time;
			retcode = utime(file_path, &ubuf);
			if (retcode < 0)
			{
				retcode = cloudfs_error("cloudfs_utime utime");
				return retcode;
			}
			INF("%s:%s Exiting:%d\n",retcode);

    			//retval = close(fi->fh);
			return retcode;

		}
	   }
    }
    retval = close(fi->fh);

    INF("%s:%s Exiting:%d\n",retval);
    ALT("%s:%s Exit fstat fi->fh :%lld\n",fi->fh); 
    return retval;
}
/* Opendir */
int cloudfs_opendir(char *path, struct fuse_file_info *fi)
{
    DIR *dp;
    int retval = 0;
    char file_path[MAX_PATH_LEN];

    INF("\n%s:%s Entering (path=\"%s\", fi=%p)\n",
          path, fi);
    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);

    dp = opendir(file_path);
    if (dp == NULL)
        retval = cloudfs_error("cloudfs_opendir opendir");

    fi->fh = (intptr_t) dp;
    
    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}
/* Access */
int cloudfs_access(char *path, int mask)
{
    int retval = 0;
    char file_path[MAX_PATH_LEN];

    INF("\n%s:%s Entering(path=\"%s\", mask=0%o)\n",
            path, mask);
    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);

    retval = access(file_path, mask);

    if (retval < 0)
        retval = cloudfs_error("cloufs_access access");

    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}
/* Utime */
int cloudfs_utime(char *path, struct utimbuf *ubuf)
{
    int retval = 0;
    int retval1 = 0;
    char file_path[MAX_PATH_LEN];
    char org_path[MAX_PATH_LEN];
    char hidden_file_path[MAX_PATH_LEN] ;
    char abs_hidden_file_path[MAX_PATH_LEN] ;
    INF("\n%s:%s Entering (path=\"%s\", ubuf=%p)\n",
            path, ubuf);
    struct stat hidden_statbuf1;
    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);
    strcpy(org_path,path);
    if (strcmp(path,"/"))
    {
           build_hidden_file_path(org_path,hidden_file_path);
           INF("%s:%s hidden file path is:%s\n",hidden_file_path);
	  /* Check if hidden file exists */
          
     	 /* Get the lstat of the hidden file if it exists */
           get_full_ssd_path(abs_hidden_file_path,hidden_file_path);
           retval1 = lstat(abs_hidden_file_path, &hidden_statbuf1);
	   INF("%s:%s lstat for hidden file(%s) retval:%d\n",abs_hidden_file_path,retval1); 
	   if (retval1 == 0)
	   {
		   /*Hidden file exists */
		   /* Get the attributes from the hidden file*/
		INF("%s:%s file exists:%s\n",abs_hidden_file_path);
		hidden_statbuf1.st_atime = ubuf -> actime; 
		hidden_statbuf1.st_mtime = ubuf -> modtime; 
                /* File read was successful */
		FILE * file = fopen(abs_hidden_file_path,"wb+");
		INF("%s:%s File in getattr\n:%p",file);
		if (file == NULL)
		{
			INF("%s:%s File in getattr is NULL\n:%p",file);
			retval = cloudfs_error("cloudfs_getattr read hidden file failed");
			return retval;
		}
		retval= fwrite((void*)&hidden_statbuf1,sizeof(struct stat),1,file);
		INF("%s:%s read ret code is :%d\n",retval);
		if (retval == 0) {
			retval = cloudfs_error("cloudfs_getattr read hidden file failed");
			return retval;
		}
		fclose(file);
	   }
    }


    retval = utime(file_path, ubuf);
    if (retval < 0)
        retval = cloudfs_error("cloudfs_utime utime");

    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}

/** Change the permission bits of a file */
int cloudfs_chmod(char *path, mode_t mode)
{
    int retval = 0;
    char file_path[MAX_PATH_LEN];

    INF("\n%s:%s Entering(file_path=\"%s\", mode=0%03o)\n",
            path, mode);
    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);

    retval = chmod(file_path, mode);
    if (retval < 0)
        retval = cloudfs_error("cloudfs_chmod chmod");

    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}

/** Remove a file */
int cloudfs_unlink(char *path)
{
    INF("\n%s:%s Entering (path=\"%s\")\n",
            path);
    int retval = 0;
   int retval1 = 0;
    char file_path[MAX_PATH_LEN];

    char org_path[MAX_PATH_LEN] ;
    char hidden_file_path[MAX_PATH_LEN] ;

    char abs_hidden_file_path[MAX_PATH_LEN] ;
    char cloud_file_name[MAX_PATH_LEN] ;

    strcpy(org_path,path);
    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);
   /* Get the cloud file name */
struct stat hidden_statbuf1;
    if (*path == '/' && *(path+1)=='.')
    {
	    return 0;
    } else 
    {
	    build_cloud_file_name(path,cloud_file_name);
	    INF("%s:%s Cloud file name is %s\n",cloud_file_name);

           build_hidden_file_path(org_path,hidden_file_path);
           INF("%s:%s hidden file path is:%s\n",hidden_file_path);
	  /* Check if hidden file exists */
          
     	 /* Get the lstat of the hidden file if it exists */
           get_full_ssd_path(abs_hidden_file_path,hidden_file_path);
           retval1 = lstat(abs_hidden_file_path, &hidden_statbuf1);
	   INF("%s:%s lstat for hidden file(%s) retval:%d\n",abs_hidden_file_path,retval1); 
	   if (retval1 == 0)
	   {
		INF("%s:%s file exists:%s\n",abs_hidden_file_path);
		/* Get the file from the bucket*/
		INF("%s:%s Getting the file from bucket:%s",cloud_file_name);

		remove_bucket(cloud_file_name);
		
		/* remove the hidden file */
		retval = unlink(abs_hidden_file_path);
    		if (retval < 0)
		{
        		retval = cloudfs_error("cloudfs_unlink unlink");
			return retval;
		}
	   }
    }
    retval = unlink(file_path);
    if (retval < 0)
        retval = cloudfs_error("cloudfs_unlink unlink");

    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}

/** Remove a directory */
int cloudfs_rmdir(char *path)
{
    int retval = 0;
    char file_path[MAX_PATH_LEN];

    INF("\n%s:%s Entering(path=\"%s\")\n",
            path);
    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);

    retval = rmdir(file_path);
    if (retval < 0)
        retval = cloudfs_error("cloudfs_rmdir rmdir");

    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}

/** Change the size of a file */
int cloudfs_truncate(char *path, off_t newsize)
{
    int retval = 0;
    char file_path[PATH_MAX];

    INF("\n%s:%s Entering(path=\"%s\", newsize=%lld)\n",
            path, newsize);
    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);

    retval = truncate(file_path, newsize);
    if (retval < 0)
        cloudfs_error("cloudfs_truncate truncate");

    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}

/*
 * Functions supported by cloudfs 
 */
static 
struct fuse_operations cloudfs_operations = {
    .init           = cloudfs_init,
    //
    // TODO
    //
    // This is where you add the VFS functions that your implementation of
    // MelangsFS will support, i.e. replace 'NULL' with 'melange_operation'
    // --- melange_getattr() and melange_init() show you what to do ...
    //
    // Different operations take different types of parameters. This list can
    // be found at the following URL:
    // --- http://fuse.sourceforge.net/doxygen/structfuse__operations.html
    //
    //
    .getattr        = (void*)cloudfs_getattr,
    .mkdir          = (void*)cloudfs_mkdir,
    .readdir        = (void*)cloudfs_readdir,
    .open           = (void*)cloudfs_open,
    .getxattr 	    = (void*)cloudfs_getxattr,
    .setxattr       = (void*)cloudfs_setxattr,
    .mknod          = (void*)cloudfs_mknod,
    .read           = (void*)cloudfs_read,
    .write          = (void*)cloudfs_write,
    .release          = (void*)cloudfs_release,
    .opendir          = (void*)cloudfs_opendir,
    .access          = (void*)cloudfs_access,
    .utime          = (void*)cloudfs_utime,
    .chmod          = (void*)cloudfs_chmod,
    .unlink          = (void*)cloudfs_unlink,
    .rmdir          = (void*)cloudfs_rmdir,
    .truncate          = (void*)cloudfs_truncate,
    .destroy        = cloudfs_destroy
};

int cloudfs_start(struct cloudfs_state *state,
                  const char* fuse_runtime_name) {

  int argc = 0;
  char* argv[10];
  argv[argc] = (char *) malloc(128 * sizeof(char));
  strcpy(argv[argc++], fuse_runtime_name);
  argv[argc] = (char *) malloc(1024 * sizeof(char));
  strcpy(argv[argc++], state->fuse_path);
  argv[argc++] = "-s"; // set the fuse mode to single thread
//  argv[argc++] = "-f"; // run fuse in foreground 
  
  state_  = *state;

  logfile = fopen("/tmp/cloudfs.log","a+");  
  setvbuf(logfile,NULL,_IOLBF,0);

  int fuse_stat = fuse_main(argc, argv, &cloudfs_operations, NULL);
    
  return fuse_stat;
}
