/* This is a wrapper around the Lustre interface which can be called directly from C++ source code.
   The standard Lustre headers can not be included in C++ due to errors converting 
   enums to ints.
*/

#include "llapi_cpp_wrapper.h"

#include <lustre/lustre_user.h>
#include <lustre/lustreapi.h>

//#include <client.h>
//#include <config.h>

#include <stdlib.h>
#include <asm/types.h>

int changelog_wrapper_start(cl_ctx_ptr *ctx, int flags,
                                 const char *mdtname, long long startrec) {
    return llapi_changelog_start(ctx, flags, mdtname, startrec);
}


int changelog_wrapper_fini(cl_ctx_ptr *ctx) {
    return llapi_changelog_fini(ctx);
}

int changelog_wrapper_recv(cl_ctx_ptr ctx,
                                changelog_rec_ptr *cr) {

    return llapi_changelog_recv(ctx, (struct changelog_rec **)cr);
}

int changelog_wrapper_free(changelog_rec_ptr *cr) {

    return llapi_changelog_free((struct changelog_rec **)cr);
}

int changelog_wrapper_clear(const char *mdtname, const char *id,
                                 long long endrec) {

    return llapi_changelog_clear(mdtname, id, endrec);
}

int get_cl_block() {
    return CHANGELOG_FLAG_BLOCK;
}

int get_cl_jobid() {

    return CHANGELOG_FLAG_JOBID;
}

int llapi_fid2path_wrapper(const char *device, const char *fidstr, char *path,
                      int pathlen, long long *recno, int *linkno) { 

    return llapi_fid2path(device, fidstr, path, pathlen, recno, linkno);
}


lustre_fid_ptr llapi_path2fid_wrapper(const char *path) {

    lustre_fid *fidptr;
    fidptr = malloc(sizeof(lustre_fid));
    int ret = llapi_path2fid(path, fidptr);
    if (ret != 0) {
        free(fidptr);
        return NULL;
    }
    return fidptr;
}


changelog_ext_rename_ptr changelog_rec_wrapper_rename(changelog_rec_ptr rec) {
    return (changelog_ext_rename_ptr)changelog_rec_rename((struct changelog_rec *)rec);
}

char *changelog_rec_wrapper_name(changelog_rec_ptr rec) {
    return changelog_rec_name((struct changelog_rec *)rec);
}    

changelog_ext_jobid_ptr changelog_rec_wrapper_jobid(changelog_rec_ptr rec) {
    return (changelog_ext_jobid_ptr)changelog_rec_jobid((struct changelog_rec *)rec);
}

size_t changelog_rec_wrapper_snamelen(changelog_rec_ptr rec) {
    return changelog_rec_snamelen((struct changelog_rec *)rec);
}

char *changelog_rec_wrapper_sname(changelog_rec_ptr rec) {
    return changelog_rec_sname((struct changelog_rec *)rec);
}

const char *changelog_type2str_wrapper(int t) {
    return changelog_type2str(t);
}

__u64 get_f_seq_from_lustre_fid(lustre_fid_ptr fid) {
    return ((lustre_fid*)(fid))->f_seq;
}

__u32 get_f_oid_from_lustre_fid(lustre_fid_ptr fid) {
    return ((lustre_fid*)(fid))->f_oid;
}
__u32 get_f_ver_from_lustre_fid(lustre_fid_ptr fid) {
    return ((lustre_fid*)(fid))->f_ver;
}

__u16 get_cr_namelen_from_changelog_rec(changelog_rec_ptr rec) {
    return ((struct changelog_rec*)(rec))->cr_namelen;
}

__u16 get_cr_flags_from_changelog_rec(changelog_rec_ptr rec) {
    return ((struct changelog_rec*)(rec))->cr_flags;
}

__u32 get_cr_type_from_changelog_rec(changelog_rec_ptr rec) {
    return ((struct changelog_rec*)(rec))->cr_type;
}

__u64 get_cr_index_from_changelog_rec(changelog_rec_ptr rec) {
    return ((struct changelog_rec*)(rec))->cr_index;
}

__u64 get_cr_prev_from_changelog_rec(changelog_rec_ptr rec) {
    return ((struct changelog_rec*)(rec))->cr_prev;
}

__u64 get_cr_time_from_changelog_rec(changelog_rec_ptr rec) {
    return ((struct changelog_rec*)(rec))->cr_time;
}

lustre_fid_ptr get_cr_tfid_from_changelog_rec(changelog_rec_ptr rec) {
    lustre_fid *fid = &(((struct changelog_rec*)rec) -> cr_tfid);
    return (lustre_fid_ptr)fid;
}

lustre_fid_ptr get_cr_pfid_from_changelog_rec(changelog_rec_ptr rec) {
    lustre_fid *fid = &(((struct changelog_rec*)rec) -> cr_pfid);
    return (lustre_fid_ptr)fid;
}

lustre_fid_ptr get_cr_sfid_from_changelog_ext_rename(changelog_ext_rename_ptr rnm_rec) {
    lustre_fid *fid = &(((struct changelog_ext_rename*)rnm_rec) -> cr_sfid);
    return (lustre_fid_ptr)fid;

}

lustre_fid_ptr get_cr_spfid_from_changelog_ext_rename(changelog_ext_rename_ptr rnm_rec) {
    lustre_fid *fid = &(((struct changelog_ext_rename*)rnm_rec) -> cr_spfid);
    return (lustre_fid_ptr)fid;

}

unsigned int get_clf_flagmask() {
    return CLF_FLAGMASK;
}

unsigned int get_clf_rename_mask() {
    return CLF_RENAME;
} 

unsigned int get_clf_jobid_mask() {
    return CLF_JOBID;
}

unsigned int get_cl_rename() {
    return CL_RENAME;
}

unsigned int get_cl_last() {
    return CL_LAST;
}

unsigned int get_cl_rmdir() {
    return CL_RMDIR;
}

unsigned int get_cl_unlink() {
    return CL_UNLINK;
}

unsigned int get_cl_create() {
    return CL_CREATE;
}

unsigned int get_cl_close() {
    return CL_CLOSE;
}

unsigned int get_cl_mkdir() {
    return CL_MKDIR;
}

unsigned int get_cl_trunc() {
    return CL_TRUNC; 
}
