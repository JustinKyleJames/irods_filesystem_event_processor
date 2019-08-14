/*
 * Main routine handling the event loop for the Beegfs changelog reader and
 * the event loop for the iRODS API client.
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

// local libraries
#include "config.hpp"
#include "irods_filesystem_event_processor_errors.hpp"
#include "logging.hpp"

// boost libraries
#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>

// beegfs headers
#include "beegfs/beegfs_file_event_log.hpp"

#include "../../common/serialized_filesystem_event.hpp" 

static std::mutex inflight_messages_mutex;
//unsigned int number_inflight_messages = 0;

namespace po = boost::program_options;

std::atomic<bool> keep_running(true);

void interrupt_handler(int dummy) {
    keep_running.store(false);
}

unsigned long long get_current_time_ns() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

//  Sends string as 0MQ string, as multipart non-terminal 
/*static bool s_sendmore (zmq::socket_t& socket, const std::string& string) {

    zmq::message_t message(string.size());
    memcpy (message.data(), string.data(), string.size());

    bool bytes_sent;
    try {
        bytes_sent= socket.send (message, ZMQ_SNDMORE);
    } catch (const zmq::error_t& e) {
        bytes_sent = 0;
    }

    return (bytes_sent > 0);
}

//  Convert string to 0MQ string and send to socket
static bool s_send(zmq::socket_t& socket, const std::string& string) {

    zmq::message_t message(string.size());
    memcpy (message.data(), string.data(), string.size());

    size_t bytes_sent;
    try {
       bytes_sent = socket.send (message);
    } catch (const zmq::error_t& e) {
        bytes_sent = 0;
    }

    return (bytes_sent > 0);
}*/

//  Receive 0MQ string from socket and convert into string
static std::string s_recv_noblock(zmq::socket_t& socket) {

    zmq::message_t message;

    try {
        socket.recv(&message, ZMQ_NOBLOCK);
    } catch (const zmq::error_t& e) {
    }

    return std::string(static_cast<char*>(message.data()), message.size());
}

class beegfs_event_read_exception : public std::exception
{
 public:
    beegfs_event_read_exception(std::string s) {
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

std::string get_basename(const std::string& p1) {
    boost::filesystem::path path_obj{p1};
    return path_obj.filename().string();
}

bool handle_event(const BeeGFS::packet& packet, const std::string& root_path, zmq::socket_t& event_aggregator_socket) {

    LOG(LOG_DBG, "packet received: [type=%s][path=%s][entryId=%s][parentEntryId=%s][targetPath=%s][targetParentId=%s]\n", 
            to_string(packet.type).c_str(), packet.path.c_str(), packet.entryId.c_str(), packet.parentEntryId.c_str(), 
            packet.targetPath.c_str(), packet.targetParentId.c_str());


    std::string full_path = concatenate_paths_with_boost(root_path, packet.path);
    std::string basename = get_basename(packet.path);
    std::string full_target_path;

    serialized_filesystem_event_t event;

    std::string event_type;

    bool skip_event = false;


    switch (packet.type) {
        case BeeGFS::FileEventType::CREATE:
            event_type = "CREATE";
            break;
        case BeeGFS::FileEventType::CLOSE_WRITE:
            event_type = "CLOSE";
            break;
        case BeeGFS::FileEventType::UNLINK:
            event_type = "UNLINK";
            break;
        case BeeGFS::FileEventType::MKDIR:
            event_type = "MKDIR";
            break;
        case BeeGFS::FileEventType::RMDIR:
            event_type = "RMDIR";
            break;
        case BeeGFS::FileEventType::RENAME:
            event_type = "RENAME";
            basename = get_basename(packet.targetPath); 
            full_target_path = concatenate_paths_with_boost(root_path, packet.targetPath);
            break;
        case BeeGFS::FileEventType::TRUNCATE:
            event_type = "TRUNCATE";
            break;
        default:
            skip_event = true;
            break;
    }

    if (skip_event) {
        return true;
    }

    create_event(get_current_time_ns(), event_type, root_path, packet.entryId, packet.parentEntryId, basename, full_target_path, full_path, event);
    
    // send event  
    zmq::message_t request(sizeof(event));
    memcpy (request.data (), &event, sizeof(event));
    event_aggregator_socket.send (request);
    
    // get the reply
    zmq::message_t reply;
    try {
        event_aggregator_socket.recv (&reply);
    } catch (const zmq::error_t& e) {
        return true;
    }

    
    // reply is either CONTINUE or PAUSE 
    LOG(LOG_INFO, "reply size:  %zu\n", reply.size());
    std::string reply_str(static_cast<char*>(reply.data()), reply.size());
    
    LOG(LOG_INFO, "reply:  %s\n", reply_str.c_str());

    return "CONTINUE" == reply_str;
}

//  Receive 0MQ string from socket and ignore the return 
void s_recv_noblock_void(zmq::socket_t& socket) {

    zmq::message_t message;
    try {
        socket.recv(&message, ZMQ_NOBLOCK);
    } catch (const zmq::error_t& e) {
    }
}

bool received_terminate_message(zmq::socket_t& subscriber) {

    s_recv_noblock_void(subscriber);
    std::string contents = s_recv_noblock(subscriber);

    return contents == "terminate";

}


// Perform a no-block message receive.  If no message is available return std::string("").
std::string receive_message(zmq::socket_t& subscriber) {

    s_recv_noblock_void(subscriber);
    std::string contents = s_recv_noblock(subscriber);

    return contents;
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
            LOG(LOG_DBG,"setting configuration file to %s\n", vm["config-file"].as<std::string>().c_str());
            config_file = vm["config-file"].as<std::string>().c_str();
        }

        if (vm.count("log-file")) {
            std::string log_file = vm["log-file"].as<std::string>();
            dbgstream = fopen(log_file.c_str(), "a");
            if (nullptr == dbgstream) {
                dbgstream = stdout;
                LOG(LOG_ERR, "could not open log file %s... using stdout instead.\n", optarg);
            } else {
                LOG(LOG_DBG, "setting log file to %s\n", vm["log-file"].as<std::string>().c_str());
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
    serialized_filesystem_event_t event;
    create_event(get_current_time_ns(), "MKDIR", config_struct.beegfs_root_path, "root", "", "", "", config_struct.beegfs_root_path, event);
    zmq::message_t request(sizeof(event)); 
    memcpy (request.data (), &event, sizeof(event));
    event_aggregator_socket.send (request);
    
    BeeGFS::FileEventReceiver receiver(config_struct.beegfs_socket.c_str());

    while (keep_running.load()) {

        // read events
        using BeeGFS::FileEventReceiver;
         
        try {
            const auto data = receiver.read(); 
            switch (data.first) {
                case FileEventReceiver::ReadErrorCode::Success:
                    while (!handle_event(data.second, config_struct.beegfs_root_path, event_aggregator_socket)) {
                        // TODO configurable sleep time
                        sleep(5);
                    }
                    break;
                case FileEventReceiver::ReadErrorCode::VersionMismatch:
                    LOG(LOG_WARN, "Invalid packet version in BeeGFS event.  Ignoring event.\n");
                    break;
                case FileEventReceiver::ReadErrorCode::InvalidSize:
                    LOG(LOG_WARN, "Invalid packet size in BeeGFS event.  Ignoring event.\n");
                    break;
                case FileEventReceiver::ReadErrorCode::ReadFailed:
                    LOG(LOG_WARN, "Read BeeGFS event failed.\n");
                    break;
        }
        } catch (BeeGFS::FileEventReceiver::exception& e) {
            LOG(LOG_INFO, "%s\n", e.what());
        }
    }
}

int main(int argc, char *argv[]) {

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
