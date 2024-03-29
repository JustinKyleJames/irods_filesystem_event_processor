/* This is a wrapper around the Lustre interface which can be called directly from C++ source code.
   The standard Lustre headers can not be included in C++ due to errors converting 
   ints to enums.
*/


#ifndef LLAPI_CPP_WRAPPER_H
#define LLAPI_CPP_WRAPPER_H

#include <stdlib.h>
#include <asm/types.h>
//#include <lustre/lustre_user.h>

typedef void* changelog_rec_ptr;
typedef void* changelog_ext_rename_ptr;
typedef void* cl_ctx_ptr;
typedef void* lustre_fid_ptr;
typedef void* changelog_ext_jobid_ptr;

int changelog_wrapper_start(cl_ctx_ptr *ctx, int flags,
                                 const char *mdtname, long long startrec); 

int changelog_wrapper_fini(cl_ctx_ptr *ctx); 

int changelog_wrapper_recv(cl_ctx_ptr ctx,
                                //changelog_rec **cr
                                changelog_rec_ptr *cr); 


int changelog_wrapper_free(changelog_rec_ptr *cr); 

int changelog_wrapper_clear(const char *mdtname, const char *id,
                                 long long endrec); 

int get_cl_block();
int get_cl_jobid();

int llapi_fid2path_wrapper(const char *device, const char *fidstr, char *path,
                      int pathlen, long long *recno, int *linkno);

lustre_fid_ptr llapi_path2fid_wrapper(const char *path);

changelog_ext_rename_ptr changelog_rec_wrapper_rename(changelog_rec_ptr rec); 

char *changelog_rec_wrapper_name(changelog_rec_ptr rec);

changelog_ext_jobid_ptr changelog_rec_wrapper_jobid(changelog_rec_ptr rec);

size_t changelog_rec_wrapper_snamelen(changelog_rec_ptr rec);

char *changelog_rec_wrapper_sname(changelog_rec_ptr rec);

const char *changelog_type2str_wrapper(int t);

__u64 get_f_seq_from_lustre_fid(lustre_fid_ptr fid);
__u32 get_f_oid_from_lustre_fid(lustre_fid_ptr fid);
__u32 get_f_ver_from_lustre_fid(lustre_fid_ptr fid);

__u16 get_cr_namelen_from_changelog_rec(changelog_rec_ptr rec);
__u16 get_cr_flags_from_changelog_rec(changelog_rec_ptr rec);
__u32 get_cr_type_from_changelog_rec(changelog_rec_ptr rec);
__u64 get_cr_index_from_changelog_rec(changelog_rec_ptr rec);
__u64 get_cr_prev_from_changelog_rec(changelog_rec_ptr rec);
__u64 get_cr_time_from_changelog_rec(changelog_rec_ptr rec);

lustre_fid_ptr get_cr_tfid_from_changelog_rec(changelog_rec_ptr rec);
lustre_fid_ptr get_cr_pfid_from_changelog_rec(changelog_rec_ptr rec);
lustre_fid_ptr get_cr_sfid_from_changelog_ext_rename(changelog_ext_rename_ptr rnm_rec);
lustre_fid_ptr get_cr_spfid_from_changelog_ext_rename(changelog_ext_rename_ptr rnm_rec);

unsigned int get_clf_flagmask();
unsigned int get_clf_rename_mask(); 
unsigned int get_clf_jobid_mask(); 

unsigned int get_cl_rename();
unsigned int get_cl_last();
unsigned int get_cl_rmdir();
unsigned int get_cl_unlink();
unsigned int get_cl_create();
unsigned int get_cl_close();
unsigned int get_cl_mkdir();
unsigned int get_cl_rmdir();
unsigned int get_cl_trunc();

#endif
