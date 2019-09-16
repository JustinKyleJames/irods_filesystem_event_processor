/*
 * Main routine handling the event loop for the Beegfs changelog reader.
 */


#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include <zmq.hpp>
#include <signal.h>
#include <thread>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <sysexits.h>
#include <utility>
#include <chrono>

// local headers 
#include "config.hpp"
#include "irods_filesystem_event_processor_errors.hpp"
#include "logging.hpp"

// boost headers 
#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>

// beegfs headers
#include "beegfs/beegfs_file_event_log.hpp"

// common headers
#include "file_system_event_avro.hpp"

// avro headers
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

extern thread_local char *thread_identifier;

namespace po = boost::program_options;

std::atomic<bool> keep_running(true);

void interrupt_handler(int dummy) {
    keep_running.store(false);
}

unsigned long long get_current_time_ns() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
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

 
std::string concatenate_paths_with_boost(const std::string& p1, const std::string& p2) {

    boost::filesystem::path path_obj_1{p1};
    boost::filesystem::path path_obj_2{p2};
    boost::filesystem::path path_result = path_obj_1/path_obj_2;
    return path_result.string();
} 

std::string get_basename(const std::string& p1) {
    boost::filesystem::path path_obj{p1};
    return path_obj.filename().string();
}

bool handle_event(const BeeGFS::packet& packet, const std::string& root_path, zmq::socket_t& event_aggregator_socket) {

    LOG(LOG_DBG, "packet received: [type=%s][path=%s][entryId=%s][parentEntryId=%s][targetPath=%s][targetParentId=%s]", 
            to_string(packet.type).c_str(), packet.path.c_str(), packet.entryId.c_str(), packet.parentEntryId.c_str(), 
            packet.targetPath.c_str(), packet.targetParentId.c_str());

    // populate event to send to even aggregator 
    fs_event::filesystem_event event;
    event.change_record_index = get_current_time_ns();
    event.root_path = root_path;
    event.object_identifier = packet.entryId;
    event.target_parent_object_identifier = packet.parentEntryId;
    event.object_name = get_basename(packet.path);
    event.target_physical_path = concatenate_paths_with_boost(root_path, packet.path);

    bool skip_event = false;

    switch (packet.type) {
        case BeeGFS::FileEventType::CREATE:
            event.event_type = "CREATE";
            break;
        case BeeGFS::FileEventType::CLOSE_WRITE:
            event.event_type = "CLOSE";
            break;
        case BeeGFS::FileEventType::UNLINK:
            event.event_type = "UNLINK";
            break;
        case BeeGFS::FileEventType::MKDIR:
            event.event_type = "MKDIR";
            break;
        case BeeGFS::FileEventType::RMDIR:
            event.event_type = "RMDIR";
            break;
        case BeeGFS::FileEventType::RENAME:
            event.event_type = "RENAME";
            event.object_name = get_basename(packet.targetPath); 
            event.source_parent_object_identifier = packet.parentEntryId;
            event.target_parent_object_identifier = packet.targetParentId;
            event.source_physical_path = concatenate_paths_with_boost(root_path, packet.path);
            event.target_physical_path = concatenate_paths_with_boost(root_path, packet.targetPath);
            break;
        case BeeGFS::FileEventType::TRUNCATE:
            event.event_type = "TRUNCATE";
            break;
        default:
            skip_event = true;
            break;
    }

    if (skip_event) {
        return true;
    }

    std::string reply_str;
    try {
        reply_str = serialize_and_send_event(event, event_aggregator_socket);
    } catch (const zmq::error_t& e) {
        return true;  // continue on error
    }

    LOG(LOG_INFO, "reply:  %s", reply_str.c_str());

    return "CONTINUE" == reply_str;
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
            std::cout << "Usage:  beegfs_event_listener [options]" << std::endl;
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
void run_main_changelog_reader_loop(const beegfs_event_listener_cfg_t& config_struct) {


    //  Prepare ZMQ context and socket
    zmq::context_t context (1);
    zmq::socket_t event_aggregator_socket (context, ZMQ_REQ);
    event_aggregator_socket.connect (config_struct.event_aggregator_address);


    // TODO:  Should we stop reading if iRODS is down.
    //   Pros:  We won't run out of memory on the filesystem_event_processor
    //   Cons:  In BeeGFS if we don't retrieve and event it is lost.

    //unsigned int sleep_period = config_struct.changelog_poll_interval_seconds;
    //

    // Add MKDIR event for root path and send to event aggregator
    fs_event::filesystem_event event;
    event.change_record_index = get_current_time_ns();
    event.event_type = "MKDIR";
    event.root_path = config_struct.beegfs_root_path; 
    event.object_identifier = "root"; 
    event.source_parent_object_identifier = "";
    event.target_parent_object_identifier = "";
    event.object_name = "";
    event.source_physical_path = "";
    event.target_physical_path = config_struct.beegfs_root_path;

    // send the event and ignore result
    try {
        serialize_and_send_event(event, event_aggregator_socket);
    } catch (const zmq::error_t& e) {
    }

    std::unique_ptr<BeeGFS::FileEventReceiver> receiver_ptr;

    try {
        receiver_ptr = std::make_unique<BeeGFS::FileEventReceiver>(config_struct.beegfs_socket.c_str());
    } catch (BeeGFS::FileEventReceiver::exception& e) {
        LOG(LOG_ERR, "%s:%d %s", __FILE__, __LINE__, e.what());
        return;
    }

    while (keep_running.load()) {

        // read events
        using BeeGFS::FileEventReceiver;
         
        try {
            const auto data = receiver_ptr->read(); 
            switch (data.first) {
                case FileEventReceiver::ReadErrorCode::Success:
                    while (!handle_event(data.second, config_struct.beegfs_root_path, event_aggregator_socket)) {
                        // TODO configurable sleep time
                        sleep(5);
                    }
                    break;
                case FileEventReceiver::ReadErrorCode::VersionMismatch:
                    LOG(LOG_WARN, "Invalid packet version in BeeGFS event.  Ignoring event.");
                    break;
                case FileEventReceiver::ReadErrorCode::InvalidSize:
                    LOG(LOG_WARN, "Invalid packet size in BeeGFS event.  Ignoring event.");
                    break;
                case FileEventReceiver::ReadErrorCode::ReadFailed:
                    LOG(LOG_WARN, "Read BeeGFS event failed.");
                    break;
            };
        } catch (BeeGFS::FileEventReceiver::exception& e) {
            LOG(LOG_ERR, "%s:%d %s", __FILE__, __LINE__, e.what());
        }

    } 
}

int main(int argc, char *argv[]) {

    thread_identifier = (char*)"main";

    std::string config_file = "beegfs_event_listener_config.json";
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

    beegfs_event_listener_cfg_t config_struct;
    rc = read_config_file(config_file, &config_struct);
    if (rc < 0) {
        return EX_CONFIG;
    }


    run_main_changelog_reader_loop(config_struct);

    return EX_OK;
}
