#define _DARWIN_C_SOURCE
#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L
#include <erl_nif.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <strings.h>
#include <time.h>

/* No descriptor or lock escapes this dirty call. In particular, caller death
 * cannot release exclusion while a separately queued rename is still pending. */
static ERL_NIF_TERM ok, error, mismatch, invalid, op_put, op_create, op_cas, op_delete;
struct error_atom { int number; ERL_NIF_TERM atom; };
static struct error_atom errors[] = {
    {EACCES,0},{EPERM,0},{ENOENT,0},{ENOTDIR,0},{EEXIST,0},{EISDIR,0},
    {ELOOP,0},{ENOSPC,0},{ENOMEM,0},{EMFILE,0},{ENFILE,0},{EROFS,0},
    {ENAMETOOLONG,0},{EINVAL,0},{EIO,0},{EXDEV,0},{ENOTSUP,0},{EINTR,0}
};
static const char *error_names[] = {
    "eacces","eperm","enoent","enotdir","eexist","eisdir","eloop",
    "enospc","enomem","emfile","enfile","erofs","enametoolong",
    "einval","eio","exdev","enotsup","eintr"
};
static ERL_NIF_TERM posix_error(ErlNifEnv *env, int e) {
    ERL_NIF_TERM atom = invalid;
    for (size_t i=0; i<sizeof(errors)/sizeof(errors[0]); i++)
        if (errors[i].number == e) { atom=errors[i].atom; break; }
    return enif_make_tuple2(env,error,atom);
}
static char *path_arg(ErlNifEnv *env, ERL_NIF_TERM arg) {
    ErlNifBinary b;
    if (!enif_inspect_binary(env,arg,&b) || b.size==0 || b.size==SIZE_MAX ||
        memchr(b.data,0,b.size)) return NULL;
    char *p=enif_alloc(b.size+1);
    if (!p) return NULL;
    memcpy(p,b.data,b.size); p[b.size]=0; return p;
}
static int basename_ok(const char *s) {
    return s && !strchr(s,'/') && strcmp(s,".") && strcmp(s,"..") &&
        strncasecmp(s,".bedrock-lock",13);
}
static int open_retry(int dir, const char *name, int flags, mode_t mode) {
    int fd;
    do { fd=openat(dir,name,flags,mode); } while(fd<0 && errno==EINTR);
    return fd;
}
#ifdef BEDROCK_TEST_BARRIERS
/* Test builds compile this SAME implementation under a different module name.
 * A stage.ready file acknowledges entry; removal of stage.release controls
 * departure. Unlinking a marker cancels even a gate not yet entered. */
static void barrier(const char *dir, const char *stage) {
    if (!dir || !*dir) return;
    char path[4096];
    if (snprintf(path,sizeof(path),"%s/%s.release",dir,stage)>=(int)sizeof(path)) return;
    if (access(path,F_OK)!=0) return;
    char ready[4096];
    if (snprintf(ready,sizeof(ready),"%s/%s.ready",dir,stage)>=(int)sizeof(ready)) return;
    int fd=open(ready,O_WRONLY|O_CREAT|O_CLOEXEC,0600);
    if(fd>=0) close(fd);
    const struct timespec pause={0,1000000};
    while(access(path,F_OK)==0) nanosleep(&pause,NULL);
}
#else
#define barrier(dir, stage) ((void)(dir))
#endif
static int same_bytes(int fd, const ErlNifBinary *expected) {
    unsigned char buf[16384]; size_t offset=0;
    for (;;) {
        ssize_t n;
        do { n=read(fd,buf,sizeof(buf)); } while(n<0 && errno==EINTR);
        if(n<0) return -1;
        if(n==0) return offset==expected->size;
        if((size_t)n>expected->size-offset || memcmp(buf,expected->data+offset,(size_t)n)) return 0;
        offset+=(size_t)n;
    }
}
static ERL_NIF_TERM mutate(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    char *dir=path_arg(env,argv[1]), *name=path_arg(env,argv[2]);
    char *scratch=NULL, *hooks=NULL;
    ErlNifBinary expected;
    int dfd=-1, lfd=-1, fd=-1, err=EINVAL;
    ERL_NIF_TERM result;
    int is_delete=enif_is_identical(argv[0],op_delete);
    int is_cas=enif_is_identical(argv[0],op_cas);
    int is_put=enif_is_identical(argv[0],op_put);
    int is_create=enif_is_identical(argv[0],op_create);
    if(!is_delete) scratch=path_arg(env,argv[3]);
#ifdef BEDROCK_TEST_BARRIERS
    hooks=path_arg(env,argv[5]);
#endif
    if(!dir || !basename_ok(name) || !(is_delete||is_cas||is_put||is_create) ||
       (!is_delete && (!scratch || strchr(scratch,'/') || strncmp(scratch,".bedrock-tmp.",13))) ||
       !enif_inspect_binary(env,argv[4],&expected)) goto failure;
    dfd=open_retry(AT_FDCWD,dir,O_RDONLY|O_DIRECTORY|O_CLOEXEC,0);
    if(dfd<0) { err=errno; goto failure; }
    /* Darwin can return ENOENT to a losing concurrent O_CREAT/openat
     * resolution. Parent is pinned; re-open the same permanent entry. */
    for(int attempt=0; attempt<3; attempt++) {
        lfd=open_retry(dfd,".bedrock-lock",O_RDWR|O_CREAT|O_NOFOLLOW|O_CLOEXEC|O_NONBLOCK,0600);
        if(lfd>=0 || errno!=ENOENT) break;
    }
    if(lfd<0) { err=errno; goto failure; }
    struct stat st;
    if(fstat(lfd,&st)<0) { err=errno; goto failure; }
    if(!S_ISREG(st.st_mode)) { err=EINVAL; goto failure; }
    struct stat lock_stat=st;
    int r;
    do { r=flock(lfd,LOCK_EX|LOCK_NB); } while(r<0 && errno==EINTR);
    if(r<0 && (errno==EWOULDBLOCK || errno==EAGAIN)) {
        barrier(hooks,"contended");
        do { r=flock(lfd,LOCK_EX); } while(r<0 && errno==EINTR);
    }
    if(r<0) { err=errno; goto failure; }
    barrier(hooks,"acquired");
    if(fstatat(dfd,name,&st,AT_SYMLINK_NOFOLLOW)==0) {
        if(st.st_dev==lock_stat.st_dev && st.st_ino==lock_stat.st_ino) { err=EINVAL; goto failure; }
        if(!S_ISREG(st.st_mode)) { err=S_ISLNK(st.st_mode)?ELOOP:EISDIR; goto failure; }
    } else if(errno!=ENOENT) { err=errno; goto failure; }
    if(is_cas) {
        fd=open_retry(dfd,name,O_RDONLY|O_NOFOLLOW|O_CLOEXEC|O_NONBLOCK,0);
        if(fd<0) { err=errno; goto failure; }
        if(fstat(fd,&st)<0) { err=errno; goto failure; }
        if(!S_ISREG(st.st_mode)) { err=EINVAL; goto failure; }
        int equal=same_bytes(fd,&expected);
        if(equal<0) { err=errno; goto failure; }
        if(!equal) { result=enif_make_tuple2(env,error,mismatch); goto cleanup; }
    }
    barrier(hooks,"publish");
    if(is_delete) r=unlinkat(dfd,name,0);
    else if(is_create) r=linkat(dfd,scratch,dfd,name,0);
    else r=renameat(dfd,scratch,dfd,name);
    if(r<0) { err=errno; goto failure; }
    result=ok; goto cleanup;
failure:
    result=posix_error(env,err);
cleanup:
    /* Never turn a published success into a failed-write claim. close releases
     * flock; do not retry close(EINTR), risking closure of a reused descriptor. */
    if(fd>=0) close(fd);
    if(lfd>=0) {
        /* Release explicitly as well as on close; a cleanup failure must not
         * turn an already published value into a definite failed-write reply. */
        int unlocked;
        do { unlocked=flock(lfd,LOCK_UN); } while(unlocked<0 && errno==EINTR);
        close(lfd);
    }
    if(dfd>=0) close(dfd);
    enif_free(dir); enif_free(name); enif_free(scratch); enif_free(hooks);
    return result;
}
static int load(ErlNifEnv *env, void **priv, ERL_NIF_TERM info) {
    (void)priv; (void)info;
    ok=enif_make_atom(env,"ok"); error=enif_make_atom(env,"error");
    mismatch=enif_make_atom(env,"version_mismatch"); invalid=enif_make_atom(env,"native_io_error");
    op_put=enif_make_atom(env,"put"); op_create=enif_make_atom(env,"create");
    op_cas=enif_make_atom(env,"cas"); op_delete=enif_make_atom(env,"delete");
    for(size_t i=0;i<sizeof(errors)/sizeof(errors[0]);i++) errors[i].atom=enif_make_atom(env,error_names[i]);
    return 0;
}
#ifdef BEDROCK_TEST_BARRIERS
static ErlNifFunc funcs[]={{"mutate",6,mutate,ERL_NIF_DIRTY_JOB_IO_BOUND}};
ERL_NIF_INIT(Elixir.Bedrock.ObjectStorage.LocalFilesystem.NativeTest,funcs,load,NULL,NULL,NULL)
#else
static ErlNifFunc funcs[]={{"mutate",5,mutate,ERL_NIF_DIRTY_JOB_IO_BOUND}};
ERL_NIF_INIT(Elixir.Bedrock.ObjectStorage.LocalFilesystem.Native,funcs,load,NULL,NULL,NULL)
#endif
