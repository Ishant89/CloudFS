//#define ALERT
#define INFO
#define CRITICAL
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
#include <sys/time.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <time.h>
#include <unistd.h>
#include "cloudfs.h"
#include <openssl/md5.h>
#include <libtar.h>
#include <../snapshot/snapshot-api.h>
#include <ftw.h>

extern "C" {
	#include "cloudapi.h"
	#include "dedup.h"
}
#include <map>
#include <iostream>
using namespace std;

#define UNUSED __attribute__((unused))
#define MD5_STR_LEN (2*MD5_DIGEST_LENGTH + 1)

#define MIN_SEG_SIZE 2048
#define MAX_SEG_SIZE 8192 
#define MICRO_SEC_FACTOR 1000
/* File open for logging */
FILE* logfile;

/* Rabin poly structure */
rabinpoly_t *rp;

/* Segment Entry on cloud */
typedef struct cloudfs_segment_entry {
	string bucket_name;
	unsigned int reference_count;
} cloudfs_segment_entry;

/* Segment entry for file */
typedef struct file_segment_entry { 
	string bucket_name;
	unsigned long segment_file_offset;
} file_segment_entry;

/** Define structures 
 * Following structures will be backed up in a file 
 * */

std::map<string,cloudfs_segment_entry> cloudfs_segment_data;
/* File count */
unsigned long file_count_on_cloud;

/* Cloud segment file */
char cloudfs_segment_file[MAX_PATH_LEN] = ".cloud_segment_data";
/* Max and min size of the rabin segment */
int min_seg_size = MIN_SEG_SIZE;
int max_seg_size = MAX_SEG_SIZE;

/* Put new hash and object name in the entry */
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

static int UNUSED cloudfs_error(const char *error_str)
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

static void get_full_ssd_path(char * full_path,const char * path)
{
        INF("\n%s:%s Entering:%s\n",__func__);
	strcpy(full_path,state_.ssd_path);
	strncat(full_path,path,MAX_PATH_LEN- strlen(path));
	INF("%s:%s Exiting:%s\n",full_path);
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
void build_cloud_file_name(const char * path,char * modified_path)
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
	outfile = fopen(file_path, "wb+");
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

/* Compute the checksum string with the given segment*/
int compute_checksum(void * buffer,unsigned int size,unsigned char * checksum)
{
	INF("%s:%s Entering:%p,%u\n",buffer,size);
	MD5_CTX  ctx;
	MD5_Init(&ctx);
	MD5_Update(&ctx,buffer,size);
	MD5_Final(checksum,&ctx);
	INF("%s:%s Entering:%s\n",checksum);
	return 0;
}

/* Print checksum */
void print_checksum(unsigned char * checksum)
{
	int i;
	for (i = 0 ; i < MD5_DIGEST_LENGTH;i++)
	{
		fprintf(stderr,"%02x",checksum[i]);
	}
	fprintf(stderr,"\n");
}
/* Get the string as checksum */
void convert_checksum_str(unsigned char * checksum,char * str_output)
{
	int i;
	for (i = 0 ; i < MD5_DIGEST_LENGTH;i++)
	{
		sprintf(str_output+ 2*i,"%02x",checksum[i]);
	}
}

/* Update file's stat to the hiddent to track cloud storage */
int update_cloud_file_stat(char * file_path,char * full_hidden_path,struct stat * org_file_stat)
{
	INF("%s:%s Entering:%s,%s\n",file_path,full_hidden_path);
	int retval = 0 ;
	/* get the file stats */
	retval = lstat(file_path,org_file_stat);
	if (retval < 0)
	{
		retval = cloudfs_error("lstat error unable to fetch large file stats");
		return retval;
	}

	/* Open a hidden file */
	int hidden_fd = open(full_hidden_path,O_CREAT|O_RDWR,
			S_IRWXU | S_IRWXG | S_IRWXO);
	INF("%s:%s Full hidden path is:%d\n",hidden_fd);
	/* Write the structure to the hidden file */
	retval = write(hidden_fd,(const void *)org_file_stat,sizeof(struct stat));
	INF("%s:%s Num of bytes written to file:%d\n",retval);
	if (retval < 0)
	{
		retval = cloudfs_error("write error stat metadata not written");
	}
	/* closing the file */
	close(hidden_fd);
	INF("%s:%s Exiting:%s,\n",file_path);
	return retval;
}
void retrieve_hashmap_from_disk()
{
	/* Open the file in w+ which will truncate */
	INF("%s:%s Entering:%s\n",__func__);
	char cloudfs_segment_full_path[MAX_PATH_LEN];
	get_full_ssd_path(cloudfs_segment_full_path,cloudfs_segment_file);

	FILE * file = fopen(cloudfs_segment_full_path,"ab+");
	if (file == NULL)
	{
		cloudfs_error("unable to open cloudfs_segment_file");
		return;
	}
	char bucket_name[MAX_PATH_LEN];
	unsigned int ref_count;
	char md5[MAX_PATH_LEN];
	string md5_key;
	string bucket_info;
	cloudfs_segment_entry entry;

	/* Restore object name count */
	fscanf(file,"%lu\n",&file_count_on_cloud);
	INF("%s:%s Buck name starts at:%lu\n",file_count_on_cloud);	
	while (!feof(file))
	{
		fscanf(file,"%s %s %u\n",md5,bucket_name,&ref_count);
		INF("%s:%s Read from file:%s,%s,%u\n",md5,bucket_name,ref_count);
		md5_key = string(md5);
		bucket_info = string(bucket_name);
		entry.bucket_name = bucket_info;
		entry.reference_count = ref_count;
		cloudfs_segment_data[md5_key] = entry;		
	}

	/* Close the file */
	fclose(file);
	INF("%s:%s Exiting:%s\n",__func__);

}
void update_segment_hashmap_disk()
{
	/* Open the file in w+ which will truncate */
	INF("%s:%s Entering:%s\n",__func__);
	char cloudfs_segment_full_path[MAX_PATH_LEN];
	get_full_ssd_path(cloudfs_segment_full_path,cloudfs_segment_file);

	FILE * file = fopen(cloudfs_segment_full_path,"wb+");
	if (file == NULL)
	{
		cloudfs_error("unable to open cloudfs_segment_file");
		return;
	}
	char str[MAX_PATH_LEN];
	/* Write the bucket count */
	sprintf(str,"%lu\n",file_count_on_cloud);
	fwrite((const void*)str,strlen(str),1,file);
	
	std::map<string,cloudfs_segment_entry>::iterator it;
	for (it = cloudfs_segment_data.begin();it != cloudfs_segment_data.end();++it)
	{
		/* Generate the string */
		sprintf(str,"%s %s %u\n",it->first.c_str(),it->second.bucket_name.c_str(),
				it->second.reference_count);
		INF("%s:%s Writing:%s\n",str);
	/* write to the file */
		fwrite((const void*)str,strlen(str),1,file);
	}

	/* Close the file */
	fclose(file);
	INF("%s:%s Exiting:%s\n",__func__);
}
/* Get the cloud objname :
 * 1. Check if the md5sum exists (If exists, get the bucket_name,increment the
 * refcount  and return )
 * 2. If does not exist, create a new map entry
 * 3. Create a new object with increasing id of value, store the data in that
 * egment
 * 4. Create a new map entry & return the bucket_name
 */

string get_cloud_objname(string md5_key,
		char * file_path,
		int segment_offset,
		int segment_len)
{
	INF("%s:%s Entering:%s,%s,%d,%d\n",md5_key.c_str(),file_path,
			segment_offset,segment_len);
	string bucket_name;
	cloudfs_segment_entry cloudfs_entry;
	if (cloudfs_segment_data.find(md5_key) != cloudfs_segment_data.end())
	{
		cloudfs_entry = cloudfs_segment_data[md5_key];
		cloudfs_entry.reference_count++;
		INF("%s:%s Ref count:%u\n",cloudfs_entry.reference_count);
		bucket_name = cloudfs_entry.bucket_name;
		INF("%s:%s bucket_name:%s\n",cloudfs_entry.bucket_name.c_str());
		cloudfs_segment_data[md5_key] = cloudfs_entry;
		/* Update segment data on disk */
//		update_segment_hashmap_disk();
		return bucket_name;
	}

	/* Entry does not exist */
	/* Get the name of the obj on cloud */
	char cloud_file[MAX_PATH_LEN];
      	sprintf(cloud_file,"%lu",file_count_on_cloud++);	

	char temp_file_path[MAX_PATH_LEN] = "/home/student/mnt/ssd/tmp";
	char buf[segment_len];
	int retval=0;
	/* Open the original  file */
	int org_file_fd = open(file_path,O_CREAT|O_RDWR,
			S_IRWXU | S_IRWXG | S_IRWXO);
	INF("%s:%s Full hidden path is:%d\n",org_file_fd);

	retval = pread(org_file_fd,(void*)buf,segment_len,segment_offset);
	INF("%s:%s Num of bytes read file:%d\n",retval);
	if (retval < 0)
	{
		retval = cloudfs_error("read error segment data not read");
		return NULL;
	}
	close(org_file_fd);

	/* Open the temp file */
	int temp_file_fd = open(temp_file_path,O_CREAT|O_RDWR,
			S_IRWXU | S_IRWXG | S_IRWXO);
	INF("%s:%s temp path is:%d\n",temp_file_fd);

	retval = pwrite(temp_file_fd,(void*)buf,segment_len,0);
	INF("%s:%s Num of bytes written file:%d\n",retval);
	if (retval < 0)
	{
		retval = cloudfs_error("write error segment data not written");
		return NULL;
	}
	close(temp_file_fd);

	/* Write segment to the cloud */
	write_to_bucket(cloud_file,segment_len,temp_file_path);

	INF("%s:%s Segment written to cloud :%s,%d\n",cloud_file,segment_len);

	/* generating the entry */
	cloudfs_entry.bucket_name = string(cloud_file);
	cloudfs_entry.reference_count = 1;
	cloudfs_segment_data[md5_key] = cloudfs_entry;
	INF("%s:%s Added entry,md5_key:%s,bucket_name:%s,ref_cound:%u\n",
			md5_key.c_str(),cloudfs_entry.bucket_name.c_str(),cloudfs_entry.reference_count);

	/* Update segment data on disk */
	//update_segment_hashmap_disk();

	/*Unlink the tmp file */
	retval = unlink(temp_file_path);
	if (retval < 0)
	{
		cloudfs_error("unlink error unable to unlink temp file ");
	}
	INF("%s:%s Exiting:%s,%s,%d,%d\n",md5_key.c_str(),file_path,
			segment_offset,segment_len);
	return cloudfs_entry.bucket_name;
}

void print_hash_map()
{
	CRTCL("%s:%s Start -------------------Hash Map----------:%s\n",__func__);
	std::map<string,cloudfs_segment_entry>::iterator it;
	for (it = cloudfs_segment_data.begin();it != cloudfs_segment_data.end();++it)
	{
		CRTCL("%s:%s key:%s,bucket_name:%s,ref_count:%u\n",it->first.c_str(),
				it->second.bucket_name.c_str(),it->second.reference_count);
	}
	CRTCL("%s:%s END -------------------Hash Map----------:%s\n",__func__);
}

/* Compute the number of segments */
void compute_file_rabin_segments(char * file_path)
{	
	
	INF("%s:%s Entering:%s\n",file_path);
	/*Copy the path */
        char full_path[MAX_PATH_LEN];
        char full_hidden_path[MAX_PATH_LEN];

	/* File stat before reading segments from the file */
	struct stat org_file_stat;

	/* Saving the file path */
	strcpy(full_path,file_path);       
		
	build_hidden_file_path(full_path,full_hidden_path);
	INF("%s:%s Full hidden path is:%s\n",full_hidden_path);

	
	/* Add curr file stat to the hidden file */
	update_cloud_file_stat(file_path,full_hidden_path,&org_file_stat);

	/* Reset Rabin segmentation*/
	rabin_reset(rp);

	/*Opening the main file to read the segments */
	int fd = open(file_path,O_CREAT|O_RDWR,
			S_IRWXU | S_IRWXG | S_IRWXO);

	INF("%s:%s Rabin file fd :%d\n",fd);

	/* Open the hidden file in append mode */
	/* Open a hidden file */
	int hidden_fd = open(full_hidden_path,O_CREAT|O_RDWR|O_APPEND,
			S_IRWXU | S_IRWXG | S_IRWXO);
	
	INF("%s:%s Hidden file in append mode is:%d\n",hidden_fd);

	if (!rp) {
		fprintf(stderr, "Failed to reset rabinhash algorithm\n");
		exit(1);
	}

	MD5_CTX ctx;
	unsigned char md5[MD5_DIGEST_LENGTH];		
	int new_segment = 0;
	int len, segment_len = 0;
	char buf[1024];
	int bytes;
	int segment_offset = 0 ;
	file_segment_entry seg_entry;
	char md5_str[MD5_STR_LEN];
	string md5_key;
	string bucket_name;
	int retval = 0;
	char segment_entry[MAX_PATH_LEN];

	MD5_Init(&ctx);
	while( (bytes = read(fd, buf, sizeof buf)) > 0 ) {
		char *buftoread = (char *)&buf[0];
		while ((len = rabin_segment_next(rp, buftoread, bytes,&new_segment)) > 0) {
			MD5_Update(&ctx, buftoread, len);
			segment_len += len;
			
			if (new_segment) {
				MD5_Final(md5, &ctx);
				/* Get the md5_str */
			        convert_checksum_str(md5,md5_str);	
				/* Get the md5 string key */
				md5_key = string(md5_str);
				/* Making the seg_entry*/
				seg_entry.segment_file_offset = segment_offset;

				/* Check if key exists in the global map table,
				 * if does not exist, put the data in the cloud 
				 * and get the objname for the corresponding 
				 * and change the reference count */
				bucket_name = get_cloud_objname(md5_key,file_path,segment_offset,segment_len);
				/* Entering the bucket name */
				seg_entry.bucket_name = bucket_name;
				
				/* Converting string to save in file */
				sprintf(segment_entry,"%lu %s %d\n",seg_entry.segment_file_offset,
						seg_entry.bucket_name.c_str(),segment_len);

				INF("%s:%s Seg info,segment_offset:%d,objname:%s\n",segment_offset,
					bucket_name.c_str());		

				/* Save the segment info in the structure */
				retval = write(hidden_fd,(const void *)segment_entry,
						strlen(segment_entry));
				INF("%s:%s Num of bytes written to file:%d\n",retval);
				if (retval < 0)
				{
					retval = cloudfs_error("write error segment metadata not written");
					return;
				}
				/* Next segment start */
				segment_offset += segment_len;

				MD5_Init(&ctx);
				segment_len = 0;
			}

			buftoread += len;
			bytes -= len;

			if (!bytes) {
				break;
			}
		}
		if (len == -1) {
			fprintf(stderr, "Failed to process the segment\n");
			exit(2);
		}
	}
	if (!new_segment)
	{
		MD5_Final(md5, &ctx);
		/* Get the md5_str */
		convert_checksum_str(md5,md5_str);	
		/* Get the md5 string key */
		md5_key = string(md5_str);
		/* Making the seg_entry*/
		seg_entry.segment_file_offset = segment_offset;

		/* Check if key exists in the global map table,
		 * if does not exist, put the data in the cloud 
		 * and get the objname for the corresponding 
		 * and change the reference count */
		bucket_name = get_cloud_objname(md5_key,file_path,segment_offset,segment_len);
		/* Entering the bucket name */
		seg_entry.bucket_name = bucket_name;
		
		INF("%s:%s Seg info,segment_offset:%d,objname:%s\n",segment_offset,
			bucket_name.c_str());		

		/* Converting string to save in file */
		sprintf(segment_entry,"%lu %s %d\n",seg_entry.segment_file_offset,
				seg_entry.bucket_name.c_str(),segment_len);
		
		/* Save the segment info in the structure */
		retval = write(hidden_fd,(const void *)segment_entry,
				strlen(segment_entry));

		INF("%s:%s Num of bytes written to file:%d\n",retval);
		
		if (retval < 0)
		{
			retval = cloudfs_error("write error segment metadata not written");
			return;
		}

	}

	/* Update segment data on disk */
	update_segment_hashmap_disk();
	/* truncating the file */
	retval = truncate(file_path,0);
	if (retval < 0)
	{
		retval = cloudfs_error("file truncate error after making segments");
		return;
	}
	INF("%s:%s File truncated:%s\n",file_path);
	/* Updating the time */
     	struct utimbuf ubuf;
	ubuf.actime = org_file_stat.st_atime;
	ubuf.modtime = org_file_stat.st_mtime;
	retval = utime(file_path, &ubuf);
	if (retval < 0)
	{
		retval = cloudfs_error("after truncating  utime error");
		return ;
	}

	INF("%s:%s File stamp changed:%s\n",file_path);
	/* Closing the file */
	close(fd);
	/* Closing the hidden file */
	close(hidden_fd);

	/* Print hash map */
	//CRTCL("%s:%s Print hash map after segments:%s\n",__func__);
	//print_hash_map();
	INF("%s:%s Exiting:%s\n",file_path);
}
void change_ref_count(char * cloud_file_name)
{
	INF("%s:%s Entering:%s\n",cloud_file_name);
	string bucket_name = string(cloud_file_name);
	//print_hash_map();
	cloudfs_segment_entry entry;
	std::map<string,cloudfs_segment_entry>::iterator it;
	for (it = cloudfs_segment_data.begin();it != cloudfs_segment_data.end();++it)
	{
		if(it->second.bucket_name ==  bucket_name)
		{
			entry = it -> second;
			entry.reference_count--;
			if (entry.reference_count == 0)
			{
				INF("%s:%s Ref count is 0, buck :%s to be removed\n",
						cloud_file_name);
				remove_bucket(cloud_file_name);
				cloudfs_segment_data.erase(it);
			} else 
			{
				INF("%s:%s Ref count decremented:%u\n",
						entry.reference_count);
				cloudfs_segment_data[it->first] = entry;
			}
		}
	}
	/* Update the has map to the file */
	update_segment_hashmap_disk();
	//print_hash_map();
	INF("%s:%s Exiting:%s\n",cloud_file_name);
}
void incr_ref_count()
{
	INF("%s:%s Entering:%s\n",__func__);
	cloudfs_segment_entry entry;
	std::map<string,cloudfs_segment_entry>::iterator it;
	for (it = cloudfs_segment_data.begin();it != cloudfs_segment_data.end();++it)
	{
		entry = it -> second;
		entry.reference_count++;
		cloudfs_segment_data[it->first] = entry;
	}
	/* Update the has map to the file */
	update_segment_hashmap_disk();
	//print_hash_map();
	INF("%s:%s Exiting:%s\n",__func__);
}
void remove_all_segments(const char * file_path,const char * hidden_file)
{
       INF("%s:%s Entering:%s,%s\n",hidden_file,file_path);
        struct stat cloud_local_file_stat;
        int retval;
	int segment_len = 0;

        unsigned long file_offset;
        char cloud_file_name[MAX_PATH_LEN];

        FILE * file = fopen(hidden_file,"rb+");
        INF("%s:%s File in getattr\n:%p",file);

        if (file == NULL)
        {
                INF("%s:%s File in getattr is NULL\n:%p",file);
                retval = cloudfs_error("cloudfs_getattr read hidden file failed");
                return ;
        }
        retval = fread((void*)&cloud_local_file_stat,
                        sizeof(struct stat),1,file);

        INF("%s:%s read ret code is :%d\n",retval);
        if (retval == 0) {
                retval = cloudfs_error("cloudfs_getattr read hidden file failed");
                return ;
        }

        while (!feof(file))
        {
                /*Find the offset and  */
                fscanf(file,"%lu %s %d\n",&file_offset,cloud_file_name,&segment_len);
                INF("%s:%s file offset:%lu,filename:%s\n",file_offset,cloud_file_name);
                /* Update the map entry for the corresponding cloud file name */
		change_ref_count(cloud_file_name);
                /* Copy the data from temp file to orginal file at offset */
        }

        fclose(file);

	//CRTCL("%s:%s after removing segments of a file:%s\n",__func__);
    	//print_hash_map();
        INF("%s:%s Exiting:%s,%s\n",hidden_file,file_path);

}

/*
 * Initializes the FUSE file system (cloudfs) by checking if the mount points
 * are valid, and if all is well, it mounts the file system ready for usage.
 *
 */
void *cloudfs_init(struct fuse_conn_info *conn UNUSED)
{
	CRTCL("%s:%s Entering:%s\n",__func__);
  cloud_init(state_.hostname);
  cloud_print_error();

  printf("Create bucket\n");
  cloud_create_bucket("cloudfs");
  cloud_print_error();
  /* Temp code */
 /* char input[50]="This is not cool";
  unsigned char checksum[MD5_DIGEST_LENGTH];
  char str_output[MD5_STR_LEN];
  compute_checksum((void*)input,50,checksum);
  print_checksum(checksum);
  convert_checksum_str(checksum,str_output);
  fprintf(stderr,"Checksum: %s\n",str_output);

  compute_file_rabin_segments("/home/student/mnt/big6");*/
  /* Define a new Map*/
	CRTCL("%s:%s Exiting:%s\n",__func__);
  return NULL;
}


void cloudfs_destroy(void *data UNUSED) {
	CRTCL("%s:%s Entering:%s\n",__func__);
  cloud_destroy();
	CRTCL("%s:%s Exiting:%s\n",__func__);
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


void copy_data_file_at_offset(const char * file_path,const char * temp_file_path, unsigned long file_offset)
{
	INF("%s:%s Entering,%s,%lu\n",file_path,file_offset);
	int bytes,retval =0;
	char buff[MAX_SEG_SIZE];

       int temp_file_fd = open(temp_file_path,O_CREAT|O_RDWR,
                        S_IRWXU | S_IRWXG | S_IRWXO);
        INF("%s:%s temp path is:%d\n",temp_file_fd);

        bytes = pread(temp_file_fd,(void*)buff,MAX_SEG_SIZE,0);
        INF("%s:%s Num of bytes read file:%d\n",bytes);
        if (bytes < 0)
        {
                retval = cloudfs_error("write error segment data not written");
                return ;
        }
        close(temp_file_fd);

	int org_file_fd = open(file_path,O_CREAT|O_RDWR,
                        S_IRWXU | S_IRWXG | S_IRWXO);
        INF("%s:%s Full hidden path is:%d\n",org_file_fd);

        retval = pwrite(org_file_fd,(void*)buff,bytes,file_offset);
        INF("%s:%s Num of bytes written file:%d\n",retval);
        if (retval < 0)
        {
                retval = cloudfs_error("read error segment data not read");
                return ;
        }
        close(org_file_fd);


	INF("%s:%s Exiting,%s,%lu\n",file_path,file_offset);
}

void get_all_segments_from_cloud(const char * hidden_file,const char * file_path)
{
	INF("%s:%s Entering:%s,%s\n",hidden_file,file_path);
	struct stat cloud_local_file_stat;
	int retval,segment_len;
	char temp_file_path[MAX_PATH_LEN]="/home/student/mnt/ssd/tmp";

	unsigned long file_offset;
	char cloud_file_name[MAX_PATH_LEN];

	FILE * file = fopen(hidden_file,"rb+");
	INF("%s:%s File in getattr\n:%p",file);
	
	if (file == NULL)
	{
		INF("%s:%s File in getattr is NULL\n:%p",file);
		retval = cloudfs_error("cloudfs_getattr read hidden file failed");
		return ;
	}
	retval = fread((void*)&cloud_local_file_stat,
			sizeof(struct stat),1,file);
	
	INF("%s:%s read ret code is :%d\n",retval);
	if (retval == 0) {
		retval = cloudfs_error("cloudfs_getattr read hidden file failed");
		return ;
	}

	while (!feof(file))
	{
		/*Find the offset and  */
		fscanf(file,"%lu %s %d\n",&file_offset,cloud_file_name,&segment_len);
		INF("%s:%s file offset:%lu,filename:%s,segment_len:%d\n",file_offset,cloud_file_name,
				segment_len);
		/* Get the data from the cloud */
		get_from_bucket(cloud_file_name,temp_file_path);

		/* Copy the data from temp file to orginal file at offset */
		copy_data_file_at_offset(file_path,temp_file_path,file_offset);
	}

	fclose(file);
        retval = unlink(temp_file_path);
        if (retval < 0)
        {
                cloudfs_error("unlink error unable to unlink temp file ");
        }

	INF("%s:%s Exiting:%s,%s\n",hidden_file,file_path);
}

/* File read */
int cloudfs_open(const char *path, struct fuse_file_info *fi)
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
    
	struct utimbuf ubuf;
if (state_.no_dedup) {
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

		if (!state_.no_dedup)
		{
			get_all_segments_from_cloud(abs_hidden_file_path,file_path);
		} else 
		{
			get_from_bucket(cloud_file_name,file_path);
		}

		/* Get the lstat of the block after getting from cloud*/

	//	retval = lstat(file_path,&cloud_local_file_stat);
	//	if (retval < 0)
	//	{
	//		retval = cloudfs_error("Unable to locate stats on file");
	//		return retval;
	//	}
                /* File read was successful */
		FILE * file = fopen(abs_hidden_file_path,"rb");
		INF("%s:%s File in getattr\n:%p",file);
		if (file == NULL)
		{
			INF("%s:%s File in getattr is NULL\n:%p",file);
			retval = cloudfs_error("cloudfs_getattr read hidden file failed");
			return retval;
		}
	   	retval = fread((void*)&cloud_local_file_stat,
				sizeof(struct stat),1,file);
		INF("%s:%s read ret code is :%d\n",retval);
		if (retval == 0) {
			retval = cloudfs_error("cloudfs_getattr read hidden file failed");
			return retval;
		}
		//closefd
		fclose(file);
		ubuf.actime = cloud_local_file_stat.st_atime;
		ubuf.modtime =cloud_local_file_stat.st_mtime;
		utime(file_path,&ubuf);
		/* Get the file descriptor of the opened file */
	 }
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
int cloudfs_mkdir(const char *path, mode_t mode)
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


int cloudfs_readdir(const char *path UNUSED, void *buf, fuse_fill_dir_t fill_dir, off_t offset UNUSED,
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
	   if (strcmp(tmp1,".") && strcmp(tmp1,"..") && (*tmp1 == '.'))
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


int cloudfs_getattr(const char *path UNUSED, struct stat *statbuf UNUSED)
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
		if (retcode < 0) 
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
    {
        //retval = cloudfs_error("cloudfs_getattr fstat");
	INF("%s:%s cloudfs_getattr fstat ;%s\n",__func__);
	retval = -errno;
    }
    INF("%s:%s Exiting:%s\n",path);
    return retval;
}

/** Get extended attributes */
int cloudfs_getxattr(const char *path,const char *name,char *value, size_t size)
{
    INF("\n%s:%s Entering:%s,%s,%s\n",path,name,value);
    int retval = 0;
    char file_path[MAX_PATH_LEN];

    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);
    retval = lgetxattr(file_path, name, value, size);
    if (retval < 0)
    {
        //retval = cloudfs_error("cloudfs_getxattr lgetxattr");
	INF("%s:%s cloudfs_getxattr lgetxattr;%s\n",__func__);
	retval = -errno;
    }
    else
        INF("%s:%s name= %s,value = \"%s\"\n", name,value);

    INF("%s:%s Exiting:%s,%s,%s\n",path,name,value);
    return retval;
}
/** Set extended attributes */
int cloudfs_setxattr(const char *path,const char *name,const char *value, size_t size,int flags)
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
int cloudfs_mknod(const char *path, mode_t mode, dev_t dev)
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
unsigned int get_file_size(const char * path)
{
	INF("%s:%s Entering:%s\n",path);
	char file_path[MAX_PATH_LEN];
	int retval= 0 ;
        struct stat file_stat;
	unsigned int size;


	get_full_ssd_path(file_path,path);

	retval = lstat(file_path,&file_stat);
	if (retval < 0)
	{
		retval = cloudfs_error("lstat error while getting the size");
		return retval;
	}
	size = file_stat.st_size;
	INF("%s:%s Exiting:%u\n",size);
	return size;
}

void get_segments(unsigned int offset,unsigned int size,const char * path)
{
	INF("%s:%s Entering:%u,%u,%s\n",offset,size,path);
	char file_path[MAX_PATH_LEN];
	char hidden_file[MAX_PATH_LEN];
	char hidden_file_path[MAX_PATH_LEN];
	char orig_path[MAX_PATH_LEN];
	struct stat cloud_file_stat;
	unsigned long file_offset;
	char  cloud_file_name[MAX_PATH_LEN];
	unsigned int start_ref = offset;
	unsigned int end_ref = offset + size;
	strcpy(orig_path,path);
	char temp_file_path[MAX_PATH_LEN]="/home/student/mnt/ssd/tmp";
	
	int segment_len = 0 ;
	int retval = 0;

	get_full_ssd_path(file_path,path);
	build_hidden_file_path(orig_path,hidden_file);
	INF("%s:%s hidden:%s\n",hidden_file);
	get_full_ssd_path(hidden_file_path,hidden_file);
	INF("%s:%s file:%s,hidden:%s\n",file_path,hidden_file_path);
	
	FILE * file = fopen(hidden_file_path,"rb+");
        INF("%s:%s File in getattr:%p\n",file);

        if (file == NULL)
        {
                INF("%s:%s File in getattr is NULL\n:%p",file);
                retval = cloudfs_error("cloudfs_getattr read hidden file failed");
                return ;
        }
        retval = fread((void*)&cloud_file_stat,
                        sizeof(struct stat),1,file);

        INF("%s:%s read ret code is :%d\n",retval);
        if (retval == 0) {
                retval = cloudfs_error("cloudfs_getattr read hidden file failed");
                return ;
        }
        while (!feof(file))
        {
                /*Find the offset and  */
                fscanf(file,"%lu %s %d\n",&file_offset,cloud_file_name,&segment_len);
                INF("%s:%s file offset:%lu,filename:%s,segment_len:%d\n",file_offset,cloud_file_name,
				segment_len);
		if (file_offset <= start_ref && start_ref < file_offset + segment_len)
		{
			/* Fetch the segment starting at the file_offset */
			INF("%s:%s offset:%lu Bucket name:%s\n",file_offset,cloud_file_name);

                	get_from_bucket(cloud_file_name,temp_file_path);
                	/* Copy the data from temp file to orginal file at offset */
                	copy_data_file_at_offset(file_path,temp_file_path,file_offset);
		} else 
		{
			if (start_ref < file_offset )
			{
				if (end_ref < file_offset)
				{
					break;
				}
				/* Fetch the segment starting at the file_offset */

			INF("%s:%s offset:%lu Bucket name:%s\n",file_offset,cloud_file_name);
				get_from_bucket(cloud_file_name,temp_file_path);
				/* Copy the data from temp file to orginal file at offset */
				copy_data_file_at_offset(file_path,temp_file_path,file_offset);

			} 
		}
	      /*  if (file_offset > start_ref)
		{
			INF("%s:%s offset:%u Bucket name:%s\n",last_segment_offset,bucket_name);
                	get_from_bucket(bucket_name,temp_file_path);

                	copy_data_file_at_offset(file_path,temp_file_path,last_segment_offset);
			if (file_offset > end_ref)
			{
				break;
			} else 
			{
				last_segment_offset = file_offset;
				strcpy(bucket_name,cloud_file_name);
				INF("%s:%s params set,off:%u,bucket name:%s\n",last_segment_offset,bucket_name);
			}

		} else 
		{
			first_segment_offset = file_offset;
			last_segment_offset  = file_offset;
			strcpy(bucket_name,cloud_file_name);
		} */
        }

        fclose(file);
        retval = unlink(temp_file_path);
        if (retval < 0)
        {
                cloudfs_error("unlink error unable to unlink temp file ");
        }

	INF("%s:%s Exiting:%u,%u,%s\n",offset,size,path);
}
int truncate_parent_file(char * file_path)
{
	INF("%s:%s Entering:%s\n",file_path);
	struct stat current_file;
	char abs_hidden_file_path[MAX_PATH_LEN];
	char full_path[MAX_PATH_LEN];
	int retcode = 0;
	strcpy(full_path,file_path);
	build_hidden_file_path(file_path,abs_hidden_file_path);
	int fd = open(abs_hidden_file_path,O_RDWR);
	INF("%s:%s FD for writing hidden file is :%d\n",fd);
	
	retcode = pread(fd,&current_file, sizeof(struct stat)
			, 0);
	if (retcode < 0)
	{
		retcode = cloudfs_error("cloudfs_close pwrite");
		return retcode;
	}

	close(fd);
	
	/* Truncating the file */
	INF("%s:%s File to be truncated:%s\n",full_path);
	retcode = truncate(full_path, 0);
	if (retcode < 0)
	{
		 retcode = cloudfs_error("cloudfs_truncate truncate");
		 return retcode;
	}

	INF("%s:%s Truncating the file:%d\n",retcode);

	struct utimbuf ubuf;
	ubuf.actime = current_file.st_atime;
	ubuf.modtime = current_file.st_mtime;
	retcode = utime(file_path, &ubuf);
	if (retcode < 0)
	{
		retcode = cloudfs_error("cloudfs_utime utime");
		return retcode;
	}
	INF("%s:%s Exiting:%s\n",file_path);
	return retcode;
}
/*Read */
int cloudfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    int retval = 0,retval1 = 0,retcode=0;
    unsigned int file_size ;
    char org_path[MAX_PATH_LEN];
    char hidden_file_path[MAX_PATH_LEN];
    struct stat hidden_statbuf1;
    char abs_hidden_file_path[MAX_PATH_LEN];
    char file_path[MAX_PATH_LEN];
    strcpy(org_path,path);
    INF("\n%s:%sEntering(path=\"%s\", buf=%p, size=%d, offset=%lld, fi=%p)\n",
            path, buf, size, offset, fi);

    if (!state_.no_dedup)
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
                retcode = fread((void*)&hidden_statbuf1,sizeof(struct stat),1,file);
                INF("%s:%s read ret code is :%d\n",retcode);
                if (retcode == 0) {
                        retcode = cloudfs_error("cloudfs_getattr read hidden file failed");
                        return retcode;
                }
                fclose(file);

		file_size = hidden_statbuf1.st_size;
		if (file_size > state_.threshold)
		{
			INF("%s:%s File size is %u\n",file_size);
			get_segments(offset,size,path);
		}		
	 }
    }
    retval = pread(fi->fh, buf, size, offset);

    /* truncating the file */
    get_full_ssd_path(file_path,path);
    if (!state_.no_dedup)
    {
	    if(retval1 == 0)
	    {

    		truncate_parent_file(file_path);
	    }
    }
    if (retval < 0)
        retval = cloudfs_error("cloudfs_read read");
    
    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}

int write_segments(unsigned int offset,unsigned int size,const char * path,const char * backup_write_file)
{
	INF("%s:%s Entering:%u,%u,%s\n",offset,size,path);
	char file_path[MAX_PATH_LEN];
	char hidden_file[MAX_PATH_LEN];
	char hidden_file_path[MAX_PATH_LEN];
	char orig_path[MAX_PATH_LEN];
	struct stat cloud_file_stat;
	unsigned long file_offset;
	char  cloud_file_name[MAX_PATH_LEN];
	unsigned int start_ref = offset;
	unsigned int end_ref = offset + size;
	strcpy(orig_path,path);
	char temp_file_path[MAX_PATH_LEN]="/home/student/mnt/ssd/tmp";
	char temp_segment_file[MAX_PATH_LEN] = "/home/student/mnt/ssd/tmp_segment";

	/* Segment start point and end point */
	unsigned long start_segment = offset;
	unsigned long end_segment = offset;
	
	int segment_len = 0;
	int retval = 0;

	get_full_ssd_path(file_path,path);
	build_hidden_file_path(orig_path,hidden_file);
	INF("%s:%s hidden:%s\n",hidden_file);
	get_full_ssd_path(hidden_file_path,hidden_file);
	INF("%s:%s file:%s,hidden:%s\n",file_path,hidden_file_path);
	
	FILE * file = fopen(hidden_file_path,"rb+");
        INF("%s:%s File in getattr:%p\n",file);

        if (file == NULL)
        {
                INF("%s:%s File in getattr is NULL\n:%p",file);
                retval = cloudfs_error("cloudfs_getattr read hidden file failed");
                return retval;
        }
        retval = fread((void*)&cloud_file_stat,
                        sizeof(struct stat),1,file);

        INF("%s:%s read ret code is :%d\n",retval);
        if (retval == 0) {
                retval = cloudfs_error("cloudfs_getattr read hidden file failed");
                return retval;
        }
	/* Opening the temp segment file */
	FILE * file1 = fopen(temp_segment_file,"wb+");
        INF("%s:%s File:%p\n",file1);

        if (file1 == NULL)
        {
                INF("%s:%s File in getattr is NULL\n:%p",file1);
                cloudfs_error("cloudfs_getattr read hidden file failed");
                return  -1 ;
        }
        while (!feof(file))
        {
                /*Find the offset and  */
                fscanf(file,"%lu %s %d\n",&file_offset,cloud_file_name,&segment_len);
                INF("%s:%s file offset:%lu,filename:%s,segment_len:%d\n",file_offset,cloud_file_name,
				segment_len);
		if (file_offset <= start_ref && start_ref < file_offset + segment_len)
		{
			/* Fetch the segment starting at the file_offset */
			INF("%s:%s obt offset:%lu Bucket name:%s\n",file_offset,cloud_file_name);
			/* Identifying the last segment */
			start_segment = file_offset;
			INF("%s:%s last segment is:%lu\n",start_segment);

                	get_from_bucket(cloud_file_name,temp_file_path);
                	/* Copy the data from temp file to orginal file at offset */
			INF("%s:%s back up offset:%lu Bucket name:%s\n",file_offset - start_segment,cloud_file_name);
                	copy_data_file_at_offset(backup_write_file,temp_file_path,file_offset - start_segment);
			/* This is the end_segment*/
			end_segment = file_offset;
			/* Change the reference count */
			change_ref_count(cloud_file_name);
		} else 
		{
			if (start_ref < file_offset )
			{
				if (end_ref < file_offset)
				{
					/* No more segments required*/
					/* Save the following segments into a
					 * file */
					INF("%s:%s last req seg:%lu & writing file_off:%lu,filename:%s,segment_len:%u\n",
							end_segment,file_offset,cloud_file_name,segment_len);
					fprintf(file1,"%lu %s %d\n",file_offset,cloud_file_name,segment_len);
					continue;
				}
				/* Fetch the segment starting at the file_offset */

				end_segment = file_offset;
				INF("%s:%s obt offset:%lu Bucket name:%s\n",file_offset,cloud_file_name);
				get_from_bucket(cloud_file_name,temp_file_path);
				/* Copy the data from temp file to orginal file at offset */
				INF("%s:%s back up offset:%lu Bucket name:%s\n",file_offset - start_segment,cloud_file_name);
				copy_data_file_at_offset(backup_write_file,temp_file_path,file_offset-start_segment);

				/* Change the reference count */
				change_ref_count(cloud_file_name);
			} 
		}
        }

	/* Closing the temp_segment_file */
	fclose(file1);
	/* Closing the hidden file */
        fclose(file);
	struct stat temp_stat;
	if (lstat(temp_file_path,&temp_stat) == 0)
	{
        	retval = unlink(temp_file_path);
		if (retval < 0)
        	{
                	cloudfs_error("unlink error unable to unlink temp file ");
        	}
	}
        
	INF("%s:%s Exiting:%u,%u,%s,%lu\n",offset,size,path,start_segment);
	return start_segment;
}

void build_metadata(const char * abs_hidden_file_path,unsigned long start_segment,struct utimbuf * ubuf)
{
	INF("%s:%s Entering:%s,%lu,%p\n",abs_hidden_file_path,start_segment,ubuf); 

        char back_up_write_file[MAX_PATH_LEN] = "/home/student/mnt/ssd/.back_up_write";
	char temp_segment_file[MAX_PATH_LEN] = "/home/student/mnt/ssd/tmp_segment";
	unsigned long file_offset;
	char cloud_file_name[MAX_PATH_LEN];
	unsigned int segment_len;
	struct stat cloud_file_stat;
	long curr_position;
	unsigned long resultant_size = 0;
	int retval = 0;
	/* Offset the hidden file where start segment starts */
        FILE * file = fopen(abs_hidden_file_path,"rb+");
	int no_seek_flag = 1;
        INF("%s:%s File opening:%s\n",abs_hidden_file_path);

        if (file == NULL)
        {
                INF("%s:%s File in getattr is NULL\n:%p",file);
                retval = cloudfs_error("cloudfs_getattr read hidden file failed");
                return ;
        }
        retval = fread((void*)&cloud_file_stat,
                        sizeof(struct stat),1,file);

        INF("%s:%s read ret code is :%d\n",retval);
        if (retval == 0) {
                retval = cloudfs_error("cloudfs_getattr read hidden file failed");
                return ;
        }

   	while (!feof(file))
        {
                /*Find the offset and  */
		curr_position = ftell(file);
		INF("%s:%s curr_pos:%ld\n",curr_position);
                fscanf(file,"%lu %s %d\n",&file_offset,cloud_file_name,&segment_len);
                INF("%s:%s file offset:%lu,filename:%s,segment_len:%d\n",file_offset,cloud_file_name,
                                segment_len);
		if (file_offset == start_segment)
		{
			INF("%s:%s seg len(offset) is:%lu\n",file_offset);
			no_seek_flag = 0;
			break;
		}
	}

	/* Seeking to the position before reading  start_segment*/
	INF("%s:%s Seeking to the pos where start_segment is:%ld\n",curr_position);
	if (no_seek_flag == 0)
	{
		fseek(file,curr_position,SEEK_SET);
	}
	/* Open the temp file */

	/* Offset the hidden file where start segment starts */
        FILE * file1 = fopen(back_up_write_file,"rb+");
        INF("%s:%s File opening:%s\n",back_up_write_file);

        if (file1 == NULL)
        {
                INF("%s:%s File in getattr is NULL\n:%p",file1);
                retval = cloudfs_error("cloudfs_getattr read hidden file failed");
                return ;
        }
        retval = fread((void*)&cloud_file_stat,
                        sizeof(struct stat),1,file1);

        INF("%s:%s read ret code is :%d\n",retval);
	if (retval == 0) {
                retval = cloudfs_error("cloudfs_getattr read hidden file failed");
                return ;
        }
	while(!feof(file1))
	{

                fscanf(file1,"%lu %s %d\n",&file_offset,cloud_file_name,&segment_len);
                INF("%s:%s writing file offset:%lu,filename:%s,segment_len:%d\n",start_segment + file_offset,
				cloud_file_name,
				segment_len);
		fprintf(file,"%lu %s %d\n",start_segment + file_offset,cloud_file_name,segment_len);
	}	
	/* Closing the backup write file */
	fclose(file1);
	/* Removing the back up write hidden file */
	unlink(back_up_write_file);

	/* Opening the temp segment file */
	FILE * file2 = fopen(temp_segment_file,"rb+");
        INF("%s:%s File:%p\n",file);

        if (file2 == NULL)
        {
                INF("%s:%s File in getattr is NULL\n:%p",file2);
                retval = cloudfs_error("cloudfs_getattr read hidden file failed");
                return ;
        }

	while(!feof(file2))
	{
		int n = fscanf(file2,"%lu %s %d\n",&file_offset,cloud_file_name,&segment_len);
                INF("%s:%s writing file offset:%lu,filename:%s,segment_len:%d\n",file_offset,cloud_file_name,
				segment_len);
		INF("%s:%s Num of elements matched:%d\n",n);
		if (n == 3)
			fprintf(file,"%lu %s %d\n",file_offset,cloud_file_name,segment_len);
	}
	/* Close the temp segment file */

	fclose(file2);

	/* Unlink the file */
	unlink(temp_segment_file);

	/* seek the file to the begining location */
	fseek(file,0,SEEK_SET);
	INF("%s:%s Seek to the first position and read the file:%ld\n",ftell(file));
	retval = fread((void*)&cloud_file_stat,
                        sizeof(struct stat),1,file);

        INF("%s:%s read ret code is :%d\n",retval);
        if (retval == 0) {
                retval = cloudfs_error("cloudfs_getattr read hidden file failed");
                return ;
        }

	while(!feof(file))
	{
		fscanf(file,"%lu %s %d\n",&file_offset,cloud_file_name,&segment_len);
                INF("%s:%s writing file offset:%lu,filename:%s,segment_len:%d\n",file_offset,cloud_file_name,
				segment_len);
		resultant_size += segment_len;
	}
	/* Changing the size */
	cloud_file_stat.st_size = resultant_size;
	INF("%s:%s Resultant size of the file:%ld\n",resultant_size);
	/* Changing the time */
	cloud_file_stat.st_atime = ubuf -> actime;
	cloud_file_stat.st_mtime = ubuf -> modtime;

	/* Seek the file to the beginning position */
	fseek(file,0,SEEK_SET);

	retval = fwrite((void*)&cloud_file_stat,
                        sizeof(struct stat),1,file);

        INF("%s:%s write ret code is :%d\n",retval);
        if (retval == 0) {
                retval = cloudfs_error("cloudfs_getattr write hidden file failed");
                return ;
        }
	/* Close the file */
	fclose(file);
	INF("%s:%s Exiting:%s,%lu,%p\n",abs_hidden_file_path,start_segment,ubuf); 
}

/* Write */

int cloudfs_write(const char *path UNUSED,const char *buf, size_t size, off_t offset,
             struct fuse_file_info *fi)
{

    INF("\n%s:%s Entering(path=\"%s\", buf=%p, size=%d, offset=%lld, fi=%p)\n",
            path, buf, size, offset, fi
            );
    int retval = 0,retval1 = 0;
    unsigned int file_size ;
    char org_path[MAX_PATH_LEN];
    char hidden_file_path[MAX_PATH_LEN];
    struct stat hidden_statbuf1;
    struct stat curr_file_stat;
    char abs_hidden_file_path[MAX_PATH_LEN];
    char file_path[MAX_PATH_LEN];
    strcpy(org_path,path);
    INF("\n%s:%sEntering(path=\"%s\", buf=%p, size=%d, offset=%lld, fi=%p)\n",
            path, buf, size, offset, fi);

    unsigned long start_segment;
    struct utimbuf ubuf;

    char back_up_write_file[MAX_PATH_LEN] = "/home/student/mnt/ssd/back_up_write";
    if (!state_.no_dedup)
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
		   /* Get the segments to backup write & get the data from cloud
		    * to that file and save the earlier segments to
		    * temp_segment_file */
			start_segment = write_segments(offset,size,path,back_up_write_file);
			/* Write to the original file */
			/* Open the backup_write_file */
			int fd = open(back_up_write_file,O_CREAT|O_RDWR,S_IRWXU | S_IRWXG | S_IRWXO);

			/* Writing to the file at particular offset */
			retval = pwrite(fd, buf, size, offset - start_segment);
		    	if (retval < 0)
			{
			    retval = cloudfs_error("cloudfs_write pwrite");
		    	    return retval;
			}
			/* Closing the file */
			close(fd);
			
			/* Get the access time and mt time of the written file
			 * */
			retval1 = lstat(back_up_write_file,&curr_file_stat);
			if (retval1 < 0)
			{
				retval1 = cloudfs_error("lstat problem");
				return retval1;
			}
			ubuf.actime = curr_file_stat.st_atime;
			ubuf.modtime = curr_file_stat.st_mtime;
			/* Compute the rabin of new segments */
			compute_file_rabin_segments(back_up_write_file);
			/* remove the write_backup_file */
			unlink(back_up_write_file);

			/* Combine the the main hidden file,
			 * back_up_write_hidden_file and temp_segment_file*/
			build_metadata(abs_hidden_file_path,start_segment,&ubuf);
			INF("%s:%s Exiting:%d\n",retval);
			return retval;
	   } else 
	   {
		    retval = pwrite(fi->fh, buf, size, offset);
		    if (retval < 0)
		    {
			    retval = cloudfs_error("cloudfs_write pwrite");
			    return retval;
		    }
		/* Compute the size of the file */
		 get_full_ssd_path(file_path,path);
		retval1 = lstat(file_path,&curr_file_stat);
		if (retval1 < 0)
		{
			return cloudfs_error("write error");
		}
		file_size = curr_file_stat.st_size;
		INF("%s:%s Size to be updated:%d\n",file_size );
		if ( file_size > state_.threshold)
		{
			compute_file_rabin_segments(file_path);
		}
		return retval;
	 }
    }

    retval = pwrite(fi->fh, buf, size, offset);
    if (retval < 0)
	    retval = cloudfs_error("cloudfs_write pwrite");

    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}


/* Release the file */
int cloudfs_release(const char *path, struct fuse_file_info *fi)
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
		/* If de dup flag is enabled */
		
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
			if (!state_.no_dedup)
			{
				/* Truncating the file */
	/*			retcode = truncate(file_path, 0);
				if (retcode < 0)
				{
					 retcode = cloudfs_error("cloudfs_truncate truncate");
					 return retcode;
				}

				INF("%s:%s Truncating the file:%d\n",retcode);

				struct utimbuf ubuf;
				ubuf.actime = cloud_file_stat.st_atime;
				ubuf.modtime = cloud_file_stat.st_mtime;
				retcode = utime(file_path, &ubuf);
				if (retcode < 0)
				{
					retcode = cloudfs_error("cloudfs_utime utime");
					return retcode;
				}  */
				  retval = close(fi->fh);
				  INF("%s:%s Exiting:%d\n",retval);
				  ALT("%s:%s Exit fstat fi->fh :%lld\n",fi->fh); 
				  return retval;
			}	/*Check the modification time */
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
			ubuf.actime = current_file.st_atime;
		   	ubuf.modtime = current_file.st_mtime;
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
		if (size_curr_file < (unsigned int)state_.threshold)
		{
			/* Remove the cloud file */
			INF("%s:%s Removing the bucket:%s",cloud_file_name);

			   if (!state_.no_dedup)
			   {
			//	   remove_all_segments(file_path,abs_hidden_file_path);
			   } else 
			   {
				remove_bucket(cloud_file_name);
			   } 
			/* Remove the hidden file */
			   if (state_.no_dedup) 
			   {
				INF("%s:%s Removing the hidden:%s",abs_hidden_file_path);
				errorcode = unlink(abs_hidden_file_path);
				if (errorcode< 0)
				{
					errorcode = cloudfs_error("cloudfs_unlink unlink");
					return errorcode;
				}
			   }
    			
			retval = close(fi->fh);
			INF("%s:%s Exiting:%d\n",retval);
			return retval;
		}

	   } else 
	   {
		  if (!state_.no_dedup)
		   {
			   if (size_curr_file >= (unsigned int)state_.threshold)
			   { 
				  /*Break into segments */
			/*	  INF("%s:%s Breaking into segments:%s\n",file_path);
				  compute_file_rabin_segments(file_path);*/
			          /* Close the file */
				  retval = close(fi->fh);
			  	  INF("%s:%s Exiting:%d\n",retval);
			          ALT("%s:%s Exit fstat fi->fh :%lld\n",fi->fh); 
			  	  return retval;

			   } 
		   } 
		   if (size_curr_file >= (unsigned int)state_.threshold)
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

    			retval = close(fi->fh);
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
int cloudfs_opendir(const char *path, struct fuse_file_info *fi)
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
int cloudfs_access(const char *path, int mask)
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
int cloudfs_utime(const char *path, struct utimbuf *ubuf)
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
                /* File read was successful */
		FILE * file = fopen(abs_hidden_file_path,"rb+");
		if (file == NULL)
		{
			INF("%s:%s File in getattr is NULL\n:%p",file);
			retval = cloudfs_error("cloudfs_getattr read hidden file failed");
			return retval;
		}
		INF("%s:%s File in getattr\n:%p",file);
		retval = fread((void*)&hidden_statbuf1,
                        sizeof(struct stat),1,file);

        	INF("%s:%s read ret code is :%d\n",retval);
        	if (retval == 0) {
                	retval = cloudfs_error("cloudfs_getattr read hidden file failed");
                	return retval;
        	}	


		hidden_statbuf1.st_atime = ubuf -> actime; 
		hidden_statbuf1.st_mtime = ubuf -> modtime; 
	
		/* Fseek to the prev location */	
		fseek(file,0,SEEK_SET);
		retval= fwrite((void*)&hidden_statbuf1,sizeof(struct stat),1,file);
		INF("%s:%s read ret code is :%d\n",retval);
		if (retval == 0) {
			retval = cloudfs_error("cloudfs_getattr read hidden file failed");
			return retval;
		}
		fclose(file);
		return 0;
	   }
    }


    retval = utime(file_path, ubuf);
    if (retval < 0)
        retval = cloudfs_error("cloudfs_utime utime");

    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}

/** Change the permission bits of a file */
int cloudfs_chmod(const char *path, mode_t mode)
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
int cloudfs_unlink(const char *path)
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
    /* Get the hidden file */
//    build_hidden_file_path(org_path,hidden_file_path);
    struct stat hidden_statbuf1;
    if (!strcmp(path,"/"))
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

		if (!state_.no_dedup)
		{
			 remove_all_segments(file_path,abs_hidden_file_path);
		} else 
		{
			remove_bucket(cloud_file_name);
		}
		
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

   // CRTCL("%s:%s after removing segments of a file:%s\n",__func__);
   // print_hash_map();
    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}

/** Remove a directory */
int cloudfs_rmdir(const char *path)
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
int cloudfs_truncate(const char *path, off_t newsize)
{
    int retval = 0;
    char file_path[PATH_MAX];
    char orig_path[MAX_PATH_LEN];
    char hidden_file_path[MAX_PATH_LEN];
    char abs_hidden_file_path[MAX_PATH_LEN];

    strcpy(orig_path,path);


    INF("\n%s:%s Entering(path=\"%s\", newsize=%lld)\n",
            path, newsize);
    /* Get the full path of the file */
    get_full_ssd_path(file_path,path);

    /* build the hidden file path*/
    build_hidden_file_path(orig_path,hidden_file_path);
    /* Get  the full hidden path */
    get_full_ssd_path(abs_hidden_file_path,hidden_file_path);

    /* Get the hidden path */
    retval = truncate(file_path, newsize);
    struct stat curr_file_stat;
    int file_size = newsize;
    if (!state_.no_dedup)
    {
    	retval = lstat(abs_hidden_file_path,&curr_file_stat);
	if (retval == 0)
	{
		if (file_size < state_.threshold)
		{
			remove_all_segments(file_path,abs_hidden_file_path);
		}	
		INF("%s:%s Removing the hidden:%s",abs_hidden_file_path);
		int errorcode = unlink(abs_hidden_file_path);
		if (errorcode< 0)
		{
			errorcode = cloudfs_error("cloudfs_unlink unlink");
			return errorcode;
		}

	}
    }
    
    if (retval < 0)
        cloudfs_error("cloudfs_truncate truncate");

    INF("%s:%s Exiting:%d\n",retval);
    return retval;
}
int make_tar(const char * src_dir,const char * tar_fname)
{
	INF("%s:%s Entering:src (%s) and tar (%s)\n",src_dir,tar_fname);
	TAR * pTar;
	char extractTo[MAX_PATH_LEN]= ".";	
	int error = 0,reterr = 0;
	error = tar_open(&pTar,tar_fname, NULL, O_WRONLY | O_CREAT, 0644, TAR_GNU);
	if (error < 0)
	{
		reterr = cloudfs_error("Tar open error");
		return reterr;
	}
        error = tar_append_tree(pTar,(char*) src_dir, extractTo);
        if (error < 0)
	{
		reterr = cloudfs_error("Tar append tree error");
		return reterr;
	}
	
	error = tar_append_eof(pTar);
        if (error < 0)
	{
		reterr = cloudfs_error("Tar eof  error");
		return reterr;
	}
	error = tar_close(pTar);
	if (error < 0)
	{
	 	reterr = cloudfs_error("Tar close error");
		return reterr;
	}
	INF("%s:%s Entering:src (%s) and tar (%s)\n",src_dir,tar_fname);
	return reterr;
}
int extract_tar(const char * final_path,const char * tar_fname)
{
	INF("%s:%s Entering:final(%s) tar(%s)\n",final_path,tar_fname);
	TAR * pTar;
	int error = 0,reterr = 0 ;
	error = tar_open(&pTar, tar_fname,NULL, O_RDONLY, 0644, TAR_GNU);
	if (error < 0)
	{
		reterr = cloudfs_error("Tar open error");
		return reterr;
	}
	error = tar_extract_all(pTar,(char*)final_path);
	if (error < 0)
	{
		reterr = cloudfs_error("Tar close error");
		return reterr;
	}
	error = tar_close(pTar);
	if (error < 0)
	{
		reterr = cloudfs_error("Tar close error");
		return reterr;
	}
	INF("%s:%s Exiting:%s\n",final_path);
}

unsigned long get_timestamp()
{
	INF("%s:%s Entering:%s\n",__func__);
	unsigned long timestamp;
	struct timeval curr_time;
	gettimeofday(&curr_time,NULL);
	timestamp = curr_time.tv_sec * MICRO_SEC_FACTOR + curr_time.tv_usec/MICRO_SEC_FACTOR;
	INF("%s:%s Entering:%lu\n",timestamp);
	return timestamp;
}
void generate_snapshot_fname(const char * snapshot_fname,unsigned long timestamp)
{
	INF("%s:%s Entering:%lu\n",timestamp);

	sprintf((char*)snapshot_fname,"snapshot_%lu.tar",timestamp);
	INF("%s:%s Exiting:%s\n",snapshot_fname);
}

void generate_snapshot_folder_install(const char * snapshot_fname,unsigned long timestamp)
{
	INF("%s:%s Entering:%lu\n",timestamp);

	sprintf((char*)snapshot_fname,"snapshot_%lu",timestamp);
	INF("%s:%s Exiting:%s\n",snapshot_fname);
}

unsigned long create_snapshot()
{
	INF("%s:%s Entering:%s\n",__func__);
	char snapshot_fname[MAX_PATH_LEN];
	char temp_tar_dir[MAX_PATH_LEN] = "/tmp/";
	unsigned long tar_file_size;
	struct stat tar_file_attr;
	int retval = 0;
	unsigned long timestamp = get_timestamp();
	/* Generate snapshot fname */
	generate_snapshot_fname(snapshot_fname,timestamp);

	strcat(temp_tar_dir,snapshot_fname);
	
	INF("%s:%s Tar file:%s\n",temp_tar_dir);

	char src_dir[MAX_PATH_LEN];
	strcpy(src_dir,state_.ssd_path);	

	retval = make_tar(src_dir,temp_tar_dir);
	if (retval < 0)
	{
		retval = cloudfs_error("Unable to tar");
		return retval;
	}

	/* Get the size of the file */
	retval = lstat(temp_tar_dir,&tar_file_attr);
	if (retval < 0)
	{
		retval = cloudfs_error("Error in stat of tar file");
		return retval;
	}
	tar_file_size = tar_file_attr.st_size;
	/* Send the tar file to the cloud */
	write_to_bucket(snapshot_fname,tar_file_size,temp_tar_dir);
	/* Increment all the reference counts and save it to hash map file */
	incr_ref_count();
	
	/* Unlink the tar file */
	unlink(temp_tar_dir);
	INF("%s:%s Exiting:%s\n",__func__);
	return timestamp;
}

/** Restore the snapshot **/

int restore_snapshot(unsigned long timestamp)
{
	INF("%s:%s Entering:%lu\n",timestamp);
	char snapshot_fname[MAX_PATH_LEN];
	char temp_tar_dir[MAX_PATH_LEN] = "/tmp/";
	int retval = 0;
	char src_dir[MAX_PATH_LEN];
	strcpy(src_dir,state_.ssd_path);	
	char rm_cmd[MAX_PATH_LEN];
	/* Generate snapshot fname */
	generate_snapshot_fname(snapshot_fname,timestamp);

	strcat(temp_tar_dir,snapshot_fname);
	
	INF("%s:%s Tar file:%s\n",temp_tar_dir);
	
	get_from_bucket((char*) snapshot_fname,temp_tar_dir);


	/* Remove the /mnt/ssd path */
	sprintf(rm_cmd,"rm -rf %s/*",src_dir);
	INF("%s:%s Rm command:%s\n",rm_cmd);
	retval = system(rm_cmd);
	if(retval < 0)
	{
		retval = cloudfs_error("Prob with rm command");
		return retval;
	}

	sprintf(rm_cmd,"rm -rf %s/.*",src_dir);
	INF("%s:%s Rm command:%s\n",rm_cmd);
	retval = system(rm_cmd);
	if(retval < 0)
	{
		retval = cloudfs_error("Prob with rm command");
		return retval;
	}
	/* Extract the tar obtained from cloud to src_dir*/
	retval = extract_tar(src_dir,temp_tar_dir);
	if (retval < 0)
	{
		retval = cloudfs_error("Unable to untar");
		return retval;
	}
	/* Unlink the tar file */
	unlink(temp_tar_dir);

	INF("%s:%s Exiting:%lu\n",timestamp);
	return 0;
}

void get_file_name(char* path,char * fname)
{
	char * temp = path;
	char * temp1 = path;
	while((temp1 = strchr(temp1,'/')))
	{
		temp = temp1;
		temp1 = temp1 + 1;
	}
	strcpy(fname,temp + 1);
}
int change_permission(const char * fpath,const struct stat * sb,
		int typeflag,struct FTW * ftwbuf)
{
	INF("%s:%s Entering:%s\n",fpath);
	if (typeflag == FTW_D)
	{
		return 0;
	}
	int retval = 0;
	char filename[MAX_PATH_LEN];
	get_file_name((char*)fpath,(char*)filename);
	if (filename[0] == '.')
	{
		return 0;
	}
	INF("%s:%s Making RO only:%s\n",fpath);
	retval = chmod(fpath,0666);
	if (retval < 0)
	{
		retval = cloudfs_error("Error in chmod");
		return retval;
	}
	INF("%s:%s Exiting:%s\n",fpath);
}

/** Install operation **/

int install_snapshot(unsigned long timestamp)
{
	INF("%s:%s Entering:%lu\n",timestamp);
	char snapshot_fname[MAX_PATH_LEN];
	char snapshot_dir_fname[MAX_PATH_LEN];
	char temp_tar_dir[MAX_PATH_LEN] = "/tmp/";
	int retval = 0;
	char src_dir[MAX_PATH_LEN];
	strcpy(src_dir,state_.ssd_path);	
	/* Generate snapshot fname */
	generate_snapshot_fname(snapshot_fname,timestamp);

	strcat(temp_tar_dir,snapshot_fname);
	
	INF("%s:%s Tar file:%s\n",temp_tar_dir);
	
	get_from_bucket((char*) snapshot_fname,temp_tar_dir);

	/* Create a new srcDIr with snapshot_xxxxxx in the path */
	generate_snapshot_folder_install(snapshot_dir_fname,timestamp);
	strcat(src_dir,snapshot_dir_fname);
	
	/* Extract the tar obtained from cloud to src_dir*/
	retval = extract_tar(src_dir,temp_tar_dir);
	if (retval < 0)
	{
		retval = cloudfs_error("Unable to untar");
		return retval;
	}
	/* Unlink the tar file */
	unlink(temp_tar_dir);
	/* Make all files Readonly*/
	retval = nftw(src_dir,change_permission,20,0);
	if (retval < 0)
	{
		retval = cloudfs_error("Error in nftw");
	}
	INF("%s:%s Exiting:%lu\n",timestamp);
	return retval;
}
/** Cloudfs ioctl (Used for Snapshot)**/

int cloudfs_ioctl(const char * path, int cmd,void * arg, 
		struct fuse_file_info * fi,unsigned int flags, void * data)
{
	CRTCL("%s:%s Entering:%s\n",path);
	printf("In ioctl and cmd is %d and %d\n",cmd,CLOUDFS_SNAPSHOT);
	unsigned long timestamp;
	int retval = 0;
	switch (cmd)
	{
		case CLOUDFS_SNAPSHOT:
			timestamp = create_snapshot();
			if (timestamp < 0)
				retval = -1;
			*(unsigned long *)data = timestamp;
			break;
		case CLOUDFS_RESTORE:
			timestamp = *(unsigned long*)data;
			retval = restore_snapshot(timestamp);
			break;
		case CLOUDFS_INSTALL_SNAPSHOT:
			timestamp = *(unsigned long*)data;
			retval = install_snapshot(timestamp);
			break;
		default:
			retval = -1;
			break;

	}
/*	TAR * pTar;
	int error=0;
	char srcDir[MAX_PATH_LEN]= "/home/student/src/t1";	
	char tar_fname[MAX_PATH_LEN] = "/home/student/ran1.tar";
	char final_path[MAX_PATH_LEN] = "/home/student/t3";
	make_tar(srcDir,tar_fname);
	extract_tar(final_path,tar_fname);	*/
	CRTCL("%s:%s Exiting:%lu\n",*(unsigned long *)data);
	return retval;
}
/*
 * Functions supported by cloudfs 
 */
static 
struct fuse_operations cloudfs_operations = {NULL};

void initialize_cloudfs_ops()
{
    cloudfs_operations.release = cloudfs_release;
    cloudfs_operations.setxattr = cloudfs_setxattr;
    cloudfs_operations.getxattr = cloudfs_getxattr;
    cloudfs_operations.opendir  = cloudfs_opendir;
    cloudfs_operations.readdir  = cloudfs_readdir;
    cloudfs_operations.init     = cloudfs_init;
    cloudfs_operations.destroy  = cloudfs_destroy;
    cloudfs_operations.utime    = cloudfs_utime;
    cloudfs_operations.access   = cloudfs_access;
    cloudfs_operations.getattr  = cloudfs_getattr;
    cloudfs_operations.mkdir    = cloudfs_mkdir;
    cloudfs_operations.open     = cloudfs_open;
    cloudfs_operations.mknod    = cloudfs_mknod;
    cloudfs_operations.read     = cloudfs_read;
    cloudfs_operations.write    = cloudfs_write;
    cloudfs_operations.chmod    = cloudfs_chmod;
    cloudfs_operations.unlink   = cloudfs_unlink;
    cloudfs_operations.rmdir    = cloudfs_rmdir;
    cloudfs_operations.truncate = cloudfs_truncate;
    cloudfs_operations.ioctl 	= cloudfs_ioctl;
}



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
  
  initialize_cloudfs_ops();
  state_  = *state;
  logfile = fopen("/tmp/cloudfs.log","a+");  
  setvbuf(logfile,NULL,_IOLBF,0);

  INF("%s:%s Entering:%s\n",__func__);
  /* If de-dup is disabled */
  if (!state_.no_dedup)
  {
	/* Initialize  the rabinpoly struct */
	  rp = rabin_init(state->rabin_window_size,state -> avg_seg_size,min_seg_size, max_seg_size);
	  if (!rp) {
	      fprintf(stderr, "Failed to init rabinhash algorithm\n");
	      exit(1);
	  }
	   CRTCL("%s:%s print hash map:%s\n",__func__); 
	   cloudfs_segment_data.clear();
	   print_hash_map();
	  INF("%s:%s Size of map is %d, file count:%lu\n",cloudfs_segment_data.size(),file_count_on_cloud);
	  retrieve_hashmap_from_disk();	
	   CRTCL("%s:%s print hash map:%s\n",__func__); 
	   print_hash_map();
  }
/** Open the snapshot file/ create it & close it */
  char snapshot_file_name [MAX_PATH_LEN] = ".snapshot";
  char snapshot_file_path[MAX_SEG_SIZE];
  get_full_ssd_path(snapshot_file_path,snapshot_file_name);

	int snapshot_fd = open(snapshot_file_path,O_CREAT|O_RDWR,
			S_IRWXU | S_IRWXG | S_IRWXO);
	INF("%s:%s Full hidden path is:%d\n",snapshot_fd);
  close(snapshot_fd);

  int fuse_stat = fuse_main(argc, argv, &cloudfs_operations, NULL);
    
  INF("%s:%s Exiting:%s\n",__func__);
  return fuse_stat;
}
