#ifndef __CLOUDFS_H_
#define __CLOUDFS_H_

#define MAX_PATH_LEN 4096
#define MAX_HOSTNAME_LEN 1024

struct cloudfs_state {
  char ssd_path[MAX_PATH_LEN];
  char fuse_path[MAX_PATH_LEN];
  char hostname[MAX_HOSTNAME_LEN];
  int ssd_size;
  int threshold;
  int avg_seg_size;
  int rabin_window_size;
  char no_dedup;
};

int cloudfs_start(struct cloudfs_state* state,
                  const char* fuse_runtime_name);  
void cloudfs_get_fullpath(const char *path, char *fullpath);


#ifdef DEBUG
#define DBG(fmt,...) fprintf(stderr,fmt,"DEBUG:",__func__,__VA_ARGS__)
#else
#define DBG(fmt,...) ((void) 0)
#endif

#ifdef CRITICAL 
#define CRTCL(fmt,...) fprintf(stderr,fmt,"CRITICAL:",__func__,__VA_ARGS__)
#else
#define CRTCL(fmt,...) ((void) 0)
#endif

#ifdef ERROR
#define ERR(fmt,...) fprintf(stderr,fmt,"ERROR:",__func__,__VA_ARGS__)
#else
#define ERR(fmt,...) ((void) 0)
#endif

#ifdef ALERT
#define ALT(fmt,...) fprintf(logfile,fmt,"ALERT:",__func__,__VA_ARGS__)
#else
#define ALT(fmt,...) ((void) 0)
#endif

#ifdef INFO 
#define INF(fmt,...) fprintf(logfile,fmt,"INF:",__func__,__VA_ARGS__)
#else
#define INF(fmt,...) ((void) 0)
#endif

#endif
