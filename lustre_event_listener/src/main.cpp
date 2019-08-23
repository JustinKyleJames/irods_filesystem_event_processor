/*
 * Main routine handling the event loop for the Lustre changelog reader.
 */

// standard headers 
#include <string>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include <zmq.hpp>
#include <sysexits.h>

// boost headers 
#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>

// local headers
#include "config.hpp"
#include "irods_filesystem_event_processor_errors.hpp"
#include "logging.hpp"
#include "main.hpp"
#include "file_system_event.hpp"

// avro headers
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

// irods headers
#include <rodsDef.h>

extern "C" {
  #include "llapi_cpp_wrapper.h"
}

extern thread_local char *thread_identifier;

namespace po = boost::program_options;

std::atomic<bool> keep_running(true);

void interrupt_handler(int dummy) {
    keep_running.store(false);
}

std::string serialize_and_send_event(const fs_event::filesystem_event& event, zmq::socket_t& socket) {

    // serialize event with avro
    std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out);
    avro::encode(*e, event); 
    boost::shared_ptr< std::vector< uint8_t > > data = avro::snapshot( *out );

    // send event to aggregator with zmq 
    zmq::message_t request(data->size());
    memcpy(request.data(), data->data(), data->size());
    socket.send(request);
    
    // get the reply
    zmq::message_t reply;
    try {
        socket.recv (&reply);
    } catch (const zmq::error_t& e) {
        throw e;
    }

    return std::string(static_cast<char*>(reply.data()), reply.size());
}


class lustre_event_read_exception : public std::exception
{
 public:
    lustre_event_read_exception(std::string s) {
        description = s;
    }   
    const char* what() const throw()
    {   
        return description.c_str();
    }   
 private:
    std::string description;
};

std::string concatenate_paths_with_boost(const std::string& p1, const std::string& p2) {

    boost::filesystem::path path_obj_1{p1};
    boost::filesystem::path path_obj_2{p2};
    boost::filesystem::path path_result = path_obj_1/path_obj_2;
    return path_result.string();
} 

static inline bool fid_is_zero(lustre_fid_ptr fid) {
    if (nullptr == fid) {
        return true;
    }
    return get_f_seq_from_lustre_fid(fid) == 0 && get_f_oid_from_lustre_fid(fid) == 0;
}

std::string get_basename(const std::string& p1) {
    boost::filesystem::path path_obj{p1};
    return path_obj.filename().string();
}

static boost::format fid_format_obj("%#llx:0x%x:0x%x");

std::string convert_to_fidstr(lustre_fid_ptr fid) {
    return str(fid_format_obj % get_f_seq_from_lustre_fid(fid) % 
            get_f_oid_from_lustre_fid(fid) % 
            get_f_ver_from_lustre_fid(fid));
}

std::string get_fidstr_from_path(std::string path) {

    lustre_fid_ptr fidptr;
    
    fidptr = llapi_path2fid_wrapper(path.c_str());
    if (fidptr != nullptr) {
        std::string fidstr = convert_to_fidstr(fidptr);
        free(fidptr);
        return fidstr;
    } else {
        return std::string();
    }
}

// for rename - the overwritten file's fidstr
std::string get_overwritten_fidstr_from_record(changelog_rec_ptr rec) {
    return convert_to_fidstr(get_cr_tfid_from_changelog_rec(rec)); 
}

int get_fidstr_from_record(changelog_rec_ptr rec, std::string& fidstr) {

    if (nullptr == rec) {
        LOG(LOG_ERR, "Null rec sent to %s - %d\n", __FUNCTION__, __LINE__);
        return irods_filesystem_event_processor_error::INVALID_OPERAND_ERROR;
    }

    changelog_ext_rename_ptr rnm;


    if (get_cr_type_from_changelog_rec(rec) == get_cl_rename()) {
        rnm = changelog_rec_wrapper_rename(rec);
        fidstr = convert_to_fidstr(get_cr_sfid_from_changelog_ext_rename(rnm));
    } else {
        fidstr = convert_to_fidstr(get_cr_tfid_from_changelog_rec(rec));
    }

    return irods_filesystem_event_processor_error::SUCCESS;

}

int get_full_path_from_record(const std::string& root_path, changelog_rec_ptr rec, std::string& lustre_full_path) {

    if (nullptr == rec) {
        LOG(LOG_ERR, "Null rec sent to %s - %d\n", __FUNCTION__, __LINE__);
        return irods_filesystem_event_processor_error::INVALID_OPERAND_ERROR;
    }

    std::string fidstr;
    long long recno = -1;
    int linkno = 0;
    int rc;

    char lustre_full_path_cstr[MAX_NAME_LEN] = {};

    // use fidstr to get path

    rc = get_fidstr_from_record(rec, fidstr);
    if (rc < 0) {
        return rc;
    }

    rc = llapi_fid2path_wrapper(root_path.c_str(), fidstr.c_str(), lustre_full_path_cstr, MAX_NAME_LEN, &recno, &linkno);

    if (rc < 0) {
        return irods_filesystem_event_processor_error::LUSTRE_OBJECT_DNE_ERROR;        
    }

    // add root path to lustre_full_path
    lustre_full_path = concatenate_paths_with_boost(root_path, lustre_full_path_cstr);

    return irods_filesystem_event_processor_error::SUCCESS;
}


bool handle_record(const std::string& lustre_root_path, changelog_rec_ptr rec, zmq::socket_t& event_aggregator_socket) {

    if (nullptr == rec) {
        LOG(LOG_ERR, "Null rec sent to %s - %d\n", __FUNCTION__, __LINE__);
        return irods_filesystem_event_processor_error::INVALID_OPERAND_ERROR;
    }

    unsigned int cr_type = get_cr_type_from_changelog_rec(rec);

    if (cr_type >= get_cl_last()) {
        LOG(LOG_ERR, "Invalid cr_type - %u\n", get_cr_type_from_changelog_rec(rec));
        return irods_filesystem_event_processor_error::INVALID_CR_TYPE_ERROR;
    }

    fs_event::filesystem_event event;
    event.root_path = lustre_root_path;
    event.index = get_cr_index_from_changelog_rec(rec);

    get_fidstr_from_record(rec, event.entryId);
    std::string lustre_full_path;
    int rc = get_full_path_from_record(lustre_root_path, rec, lustre_full_path);
    if (irods_filesystem_event_processor_error::SUCCESS != rc && irods_filesystem_event_processor_error::LUSTRE_OBJECT_DNE_ERROR != rc) {
        return rc;
    }

    event.targetParentId = convert_to_fidstr(get_cr_pfid_from_changelog_rec(rec));

    event.basename = get_basename(lustre_full_path);
    event.full_path = concatenate_paths_with_boost(lustre_root_path, lustre_full_path);


    // TODO object name vs basename
    std::string object_name(changelog_rec_wrapper_name(rec), get_cr_namelen_from_changelog_rec(rec));

    if (cr_type == get_cl_create()) {
        event.event_type = "CREATE";
    } else if (cr_type == get_cl_close()) {
        event.event_type = "CLOSE";
    } else if (cr_type == get_cl_unlink()) {
        event.event_type = "UNLINK";
    } else if (cr_type == get_cl_mkdir()) {
        event.event_type = "MKDIR";
    } else if (cr_type == get_cl_rmdir()) {
        event.event_type = "RMDIR";
    } else if (cr_type == get_cl_rename()) {

        std::string old_filename;
        std::string old_lustre_path;
        std::string old_parent_fid;
        std::string old_parent_path;

        changelog_ext_rename_ptr rnm = changelog_rec_wrapper_rename(rec);
        long long recno = -1;
        int linkno = 0;

        old_filename = std::string(changelog_rec_wrapper_sname(rec), changelog_rec_wrapper_snamelen(rec));
        old_parent_fid = convert_to_fidstr(get_cr_spfid_from_changelog_ext_rename(rnm));

        char old_parent_path_cstr[MAX_NAME_LEN] = {};
        rc = llapi_fid2path_wrapper(lustre_root_path.c_str(), old_parent_fid.c_str(), old_parent_path_cstr, MAX_NAME_LEN, &recno, &linkno);
        if (rc < 0) {
            // print error and continue
            LOG(LOG_ERR, "in rename - llapi_fid2path in %s returned an error.", __FUNCTION__);
            return true;
        } 

        // add a leading '/' if necessary
        if (0 == old_parent_path.length() || '/' != old_parent_path[0]) {
            old_parent_path = "/" + old_parent_path;
        }
        // add a trailing '/' if necessary
        if ('/' != old_parent_path[old_parent_path.length()-1]) {
             old_parent_path += "/";
        }
        event.event_type = "RENAME";

        // for renames, the old path goes in full_path and the new name goes is full_target_path
        old_lustre_path = lustre_root_path + old_parent_path + old_filename;
        event.full_target_path = event.full_path; 
        event.full_path = old_lustre_path;

    } else if (cr_type == get_cl_trunc()) {
        event.event_type = "TRUNCATE";
    } else {
        // event we don't care about, just continue reading events 
        return true; 
    }

    // TODO remove -  for debug just return
    return true;

    std::string reply_str;
    try {
        reply_str = serialize_and_send_event(event, event_aggregator_socket);
    } catch (const zmq::error_t& e) {
        return true;  // continue on error
    }

    LOG(LOG_INFO, "reply:  %s", reply_str.c_str());

    return "CONTINUE" == reply_str;
}

int start_changelog(const std::string& mdtname, cl_ctx_ptr *ctx, unsigned long long start_cr_index) {
    // TODO passing start_cr_index seems to not work
    return changelog_wrapper_start(ctx, get_cl_block(), mdtname.c_str(), start_cr_index);
}

int finish_changelog(cl_ctx_ptr *ctx) {
    return changelog_wrapper_fini(ctx);
}

int read_and_process_command_line_options(int argc, char *argv[], std::string& config_file) {
   
    po::options_description desc("Allowed options");
    try { 

        desc.add_options()
            ("help,h", "produce help message")
            ("config-file,c", po::value<std::string>(), "configuration file")
            ("log-file,l", po::value<std::string>(), "log file");
                                                                                                ;
        po::positional_options_description p;
        p.add("input-file", -1);
        
        po::variables_map vm;

        // read the command line arguments
        po::store(po::command_line_parser(argc, argv).options(desc).positional(p).run(), vm);
        po::notify(vm);

        if (vm.count("help")) {
            std::cout << "Usage:  lustre_event_listener [options]" << std::endl;
            std::cout << desc << std::endl;
            return irods_filesystem_event_processor_error::QUIT;
        }

        if (vm.count("config-file")) {
            LOG(LOG_DBG,"setting configuration file to %s", vm["config-file"].as<std::string>().c_str());
            config_file = vm["config-file"].as<std::string>().c_str();
        }

        if (vm.count("log-file")) {
            std::string log_file = vm["log-file"].as<std::string>();
            dbgstream = fopen(log_file.c_str(), "a");
            if (nullptr == dbgstream) {
                dbgstream = stdout;
                LOG(LOG_ERR, "could not open log file %s... using stdout instead.", optarg);
            } else {
                LOG(LOG_DBG, "setting log file to %s", vm["log-file"].as<std::string>().c_str());
            }
        }
        return irods_filesystem_event_processor_error::SUCCESS;
    } catch (std::exception& e) {
         std::cerr << e.what() << std::endl;
         std::cerr << desc << std::endl;
         return irods_filesystem_event_processor_error::INVALID_OPERAND_ERROR;
    }

}

// This is the main changelog reader loop.  It reads changelogs and sends results to the
// event aggregator.
int run_main_changelog_reader_loop(const lustre_event_listener_cfg_t& config_struct) {

    unsigned long long last_cr_index = 0;

    //  Prepare ZMQ context and socket
    zmq::context_t context (1);
    zmq::socket_t event_aggregator_socket (context, ZMQ_REQ);
    event_aggregator_socket.connect (config_struct.event_aggregator_address);

    // Add MKDIR event for root path and send to event aggregator
    fs_event::filesystem_event event;
    event.index = 0;
    event.event_type = "MKDIR";
    event.root_path = config_struct.lustre_root_path; 
    event.entryId = get_fidstr_from_path(config_struct.lustre_root_path); 
    event.targetParentId = "";
    event.basename = "";
    event.full_target_path = "";
    event.full_path = config_struct.lustre_root_path;

    // send the event and ignore result
    try {
        serialize_and_send_event(event, event_aggregator_socket);
    } catch (const zmq::error_t& e) {
    }

    int rc;
    changelog_rec_ptr rec;

    // changelog reader context
    cl_ctx_ptr reader_ctx;

    // start changelog
    rc = start_changelog(config_struct.mdtname, &reader_ctx, last_cr_index);
    if (rc < 0) {
        LOG(LOG_ERR, "changelog_start: %s  [rc=%d]\n", zmq_strerror(-rc), rc);
       
    }    

    while (keep_running.load()) {

        if (nullptr == reader_ctx) {
            rc = start_changelog(config_struct.mdtname, &reader_ctx, last_cr_index);
 
            if (rc < 0) {
                LOG(LOG_ERR, "changelog_start: %s\n", zmq_strerror(-rc));
                return irods_filesystem_event_processor_error::CHANGELOG_START_ERROR;
            }   
        } 

        // read events
        
        rc = changelog_wrapper_recv(reader_ctx, &rec);

        if (1 == rc || -EAGAIN == rc || -EPROTO == rc) {

             // reset the connection and try again
             finish_changelog(&reader_ctx);
             reader_ctx = nullptr;

             rc = start_changelog(config_struct.mdtname, &reader_ctx, last_cr_index);
 
             if (rc < 0) {
                 LOG(LOG_ERR, "changelog_start: %s\n", zmq_strerror(-rc));
                 return irods_filesystem_event_processor_error::CHANGELOG_START_ERROR;
             }   
        } else if (0 != rc) {
            return irods_filesystem_event_processor_error::CHANGELOG_READ_ERROR;
        }

        time_t      secs;
        struct tm   ts;

        secs = get_cr_time_from_changelog_rec(rec) >> 30;
        gmtime_r(&secs, &ts);

        lustre_fid_ptr cr_tfid_ptr = get_cr_tfid_from_changelog_rec(rec);

        LOG(LOG_INFO, "%llu %02d%-5s %02d:%02d:%02d.%06d %04d.%02d.%02d 0x%x t=%#llx:0x%x:0x%x",
               get_cr_index_from_changelog_rec(rec), get_cr_type_from_changelog_rec(rec), 
               changelog_type2str_wrapper(get_cr_type_from_changelog_rec(rec)), 
               ts.tm_hour, ts.tm_min, ts.tm_sec,
               (int)(get_cr_time_from_changelog_rec(rec) & ((1 << 30) - 1)),
               ts.tm_year + 1900, ts.tm_mon + 1, ts.tm_mday,
               get_cr_flags_from_changelog_rec(rec) & get_clf_flagmask(), 
               get_f_seq_from_lustre_fid(cr_tfid_ptr),
               get_f_oid_from_lustre_fid(cr_tfid_ptr),
               get_f_ver_from_lustre_fid(cr_tfid_ptr));

        if (get_cr_flags_from_changelog_rec(rec) & get_clf_jobid_mask()) {
            LOG(LOG_INFO, " j=%s", (const char *)changelog_rec_wrapper_jobid(rec));
        }

        if (get_cr_flags_from_changelog_rec(rec) & get_clf_rename_mask()) { 
            changelog_ext_rename_ptr rnm;

            rnm = changelog_rec_wrapper_rename(rec);
            if (!fid_is_zero(get_cr_sfid_from_changelog_ext_rename(rnm))) {
                lustre_fid_ptr cr_sfid_ptr = get_cr_sfid_from_changelog_ext_rename(rnm);
                lustre_fid_ptr cr_spfid_ptr = get_cr_spfid_from_changelog_ext_rename(rnm);
                LOG(LOG_DBG, " s=%#llx:0x%x:0x%x sp=%#llx:0x%x:0x%x %.*s", 
                       get_f_seq_from_lustre_fid(cr_sfid_ptr),
                       get_f_oid_from_lustre_fid(cr_sfid_ptr),
                       get_f_ver_from_lustre_fid(cr_sfid_ptr),
                       get_f_seq_from_lustre_fid(cr_spfid_ptr),
                       get_f_oid_from_lustre_fid(cr_spfid_ptr),
                       get_f_ver_from_lustre_fid(cr_spfid_ptr),
                       (int)changelog_rec_wrapper_snamelen(rec),
                       changelog_rec_wrapper_sname(rec));
            }
        }

        // if rename
        if (get_cr_namelen_from_changelog_rec(rec)) {
            lustre_fid_ptr cr_pfid_ptr = get_cr_pfid_from_changelog_rec(rec);
            LOG(LOG_DBG, " p=%#llx:0x%x:0x%x %.*s", 
                    get_f_seq_from_lustre_fid(cr_pfid_ptr),
                    get_f_oid_from_lustre_fid(cr_pfid_ptr),
                    get_f_ver_from_lustre_fid(cr_pfid_ptr),
                    get_cr_namelen_from_changelog_rec(rec),
                    changelog_rec_wrapper_name(rec));
        }

        rc = handle_record(config_struct.lustre_root_path, rec, event_aggregator_socket);
        if (rc < 0) {
            lustre_fid_ptr cr_tfid_ptr = get_cr_tfid_from_changelog_rec(rec);
            LOG(LOG_ERR, "handle record failed for %s %#llx:0x%x:0x%x rc = %i\n", 
                    changelog_type2str_wrapper(get_cr_type_from_changelog_rec(rec)), 
                    get_f_seq_from_lustre_fid(cr_tfid_ptr),
                    get_f_oid_from_lustre_fid(cr_tfid_ptr),
                    get_f_ver_from_lustre_fid(cr_tfid_ptr),
                    rc);
        }

        last_cr_index = get_cr_index_from_changelog_rec(rec);
        rc = changelog_wrapper_free(&rec);
        if (rc < 0) {
            LOG(LOG_ERR, "changelog_free: %s\n", zmq_strerror(-rc));
        }
         
        rc = changelog_wrapper_clear(config_struct.mdtname.c_str(), config_struct.changelog_reader.c_str(), last_cr_index);
        if (rc < 0) {
            LOG(LOG_ERR, "changelog_clear: %s\n", zmq_strerror(-rc));
        }

    }

    return irods_filesystem_event_processor_error::SUCCESS;

}

int main(int argc, char *argv[]) {

    thread_identifier = (char*)"main";

    std::string config_file = "lustre_event_listener_config.json";
    std::string log_file;

    signal(SIGPIPE, SIG_IGN);
    
    struct sigaction sa;
    memset( &sa, 0, sizeof(sa) );
    sa.sa_handler = interrupt_handler;
    sigfillset(&sa.sa_mask);
    sigaction(SIGINT,&sa,NULL);

    int rc;

    rc = read_and_process_command_line_options(argc, argv, config_file);
    if (irods_filesystem_event_processor_error::QUIT == rc) {
        return EX_OK;
    } else if (irods_filesystem_event_processor_error::INVALID_OPERAND_ERROR == rc) {
        return  EX_USAGE;
    }

    lustre_event_listener_cfg_t config_struct;
    rc = read_config_file(config_file, &config_struct);
    if (rc < 0) {
        return EX_CONFIG;
    }

    run_main_changelog_reader_loop(config_struct);

    return EX_OK;
}
