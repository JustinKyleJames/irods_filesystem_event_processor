/*
 * Main code for the event accumulator.
 *   - receives events from filesystem
 *   - accumulates them
 *   - has threads send batch updates to iRODS 
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

// local headers 
#include "irods_ops.hpp"
#include "change_table.hpp"
#include "config.hpp"
#include "logging.hpp"

// common headers 
#include "irods_filesystem_event_processor_errors.hpp"
#include "file_system_event.hpp"

// irods libraries
#include "rodsDef.h"
#include "inout_structs.h"

// boost headers 
#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>

// avro headers
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

static std::mutex inflight_messages_mutex;
unsigned int number_inflight_messages = 0;

namespace po = boost::program_options;

std::atomic<bool> keep_running(true);

void interrupt_handler(int dummy) {
    keep_running.store(false);
}

//  Sends string as 0MQ string, as multipart non-terminal 
static bool s_sendmore (zmq::socket_t& socket, const std::string& string) {

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
}

//  Receive 0MQ string from socket and convert into string
static std::string s_recv_noblock(zmq::socket_t& socket) {

    zmq::message_t message;

    try {
        socket.recv(&message, ZMQ_NOBLOCK);
    } catch (const zmq::error_t& e) {
    }

    return std::string(static_cast<char*>(message.data()), message.size());
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
            std::cout << "Usage:  filesystem_event_aggregator [options]" << std::endl;
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

// thread which reads the results from the irods updater threads and updates
// the change table in memory
void result_accumulator_main(const filesystem_event_aggregator_cfg_t *config_struct_ptr,
        change_map_t* change_map, std::set<std::string>* active_objectIdentifier_list) {

    if (nullptr == change_map || nullptr == config_struct_ptr) {
        LOG(LOG_ERR, "result accumulator received a nullptr and is exiting.");
        return;
    }


    // set up broadcast subscriber for terminate messages 
    zmq::context_t context(1);  // 1 I/O thread
    zmq::socket_t subscriber(context, ZMQ_SUB);
    LOG(LOG_DBG, "result_accumulator subscriber conn_str = %s\n", config_struct_ptr->irods_client_broadcast_address.c_str());
    subscriber.connect(config_struct_ptr->irods_client_broadcast_address);
    std::string identity("changetable_readers");
    subscriber.setsockopt(ZMQ_SUBSCRIBE, identity.c_str(), identity.length());

    // set up receiver to receive results
    zmq::socket_t receiver(context,ZMQ_PULL);
    receiver.setsockopt(ZMQ_RCVTIMEO, config_struct_ptr->message_receive_timeout_msec);
    LOG(LOG_DBG, "result_accumulator receiver conn_str = %s\n", config_struct_ptr->result_accumulator_push_address.c_str());
    receiver.bind(config_struct_ptr->result_accumulator_push_address);
    receiver.connect(config_struct_ptr->result_accumulator_push_address);

    while (true) {
        zmq::message_t message;

        size_t bytes_received = 0;
        try {
            bytes_received = receiver.recv(&message);
        } catch (const zmq::error_t& e) {
            bytes_received = 0;
        }

        if (bytes_received > 0) {

            {
                std::lock_guard<std::mutex> lock(inflight_messages_mutex);
                number_inflight_messages--;
            }

            LOG(LOG_DBG, "accumulator received message of size: %lu.\n", message.size());
            unsigned char *buf = static_cast<unsigned char*>(message.data());
            std::string update_status;
            get_update_status_from_avro_buf(buf, message.size(), update_status);
            LOG(LOG_INFO, "accumulator received update status of %s\n", update_status.c_str());

            if (update_status == "FAIL") {
                add_avro_buffer_back_to_change_table(buf, message.size(), *change_map, *active_objectIdentifier_list);
            } else {
                // remove all objectIdentifier from active_objectIdentifier_list 
                remove_objectId_from_active_list(buf, message.size(), *active_objectIdentifier_list);
            } 
            /*char response_flag[5];
            memcpy(response_flag, message.data(), 4);
            response_flag[4] = '\0';
            LOG(LOG_DBG, "response_flag is %s\n", response_flag);
            unsigned char *tmp= static_cast<unsigned char*>(message.data());
            unsigned char *response_buffer = tmp + 4;

            if (0 == strcmp(response_flag, "FAIL")) {
                add_avro_buffer_back_to_change_table(response_buffer, message.size() - 4, *change_map);
            }*/
        } 

        if ("terminate" == receive_message(subscriber)) {
            LOG(LOG_DBG, "result accumulator received a terminate message\n");
            break;
        }
    }
    LOG(LOG_DBG, "result accumulator exiting\n");


}

// irods api client thread main routine
// this is the main loop that reads the change entries in memory and sends them to iRODS via the API.
void irods_api_client_main(const filesystem_event_aggregator_cfg_t *config_struct_ptr,
        change_map_t* change_map, unsigned int thread_number, std::set<std::string>* active_objectIdentifier_list) {

    if (nullptr == change_map || nullptr == config_struct_ptr) {
        LOG(LOG_ERR, "irods api client received a nullptr and is exiting.");
        return;
    }

    // set up broadcast subscriber for terminate messages
    zmq::context_t context(1);  // 1 I/O thread
    zmq::socket_t subscriber(context, ZMQ_SUB);
    LOG(LOG_DBG, "client (%u) subscriber conn_str = %s\n", thread_number, config_struct_ptr->irods_client_broadcast_address.c_str());
    subscriber.connect(config_struct_ptr->irods_client_broadcast_address.c_str());
    std::string identity("changetable_readers");
    subscriber.setsockopt(ZMQ_SUBSCRIBE, identity.c_str(), identity.length());

    // set up broadcast publisher for sending pause message to log reader in case of irods failures
    //zmq::context_t context2(1);
    zmq::socket_t publisher(context, ZMQ_PUB);
    LOG(LOG_DBG, "client (%u) publisher conn_str = %s\n", thread_number, config_struct_ptr->changelog_reader_broadcast_address.c_str());
    publisher.connect(config_struct_ptr->changelog_reader_broadcast_address.c_str());

    // set up sender for sending update result status
    zmq::socket_t sender(context, ZMQ_PUSH);
    LOG(LOG_DBG, "client (%u) push results conn_str = %s\n", thread_number, config_struct_ptr->result_accumulator_push_address.c_str());
    sender.connect(config_struct_ptr->result_accumulator_push_address.c_str());

    bool quit = false;
    bool irods_error_detected = false;

    while (!quit) {

        // initiate a connection object
        irods_connection conn(thread_number);


        // TODO continue
        while (entries_ready_to_process(*change_map)) {

            LOG(LOG_DBG, "Client (%u) getting entries from changemap\n", thread_number);

            // get records ready to be processed into buf and buflen
            boost::shared_ptr< std::vector<uint8_t>> buffer;
            int rc = write_change_table_to_avro_buf(config_struct_ptr, buffer,
                    *change_map, *active_objectIdentifier_list);


            // if we had a collision (meaning a dependency was encountered) avoid a busy-wait by breaking out of the
            // loop where we can sleep
            if (rc == irods_filesystem_event_processor_error::COLLISION_IN_FIDSTR) {
                LOG(LOG_INFO, "Client (%u) ----- Collision!  Breaking out -----\n", thread_number);
                break;
            }

			if (!irods_error_detected && buffer->size() > 0) {

				irodsFsEventApiInp_t inp {};
				inp.buf = static_cast<unsigned char*>(buffer->data());
				inp.buflen = buffer->size(); 

				if (0 == conn.instantiate_irods_connection(config_struct_ptr, thread_number )) {

					// send to irods
                    LOG(LOG_DBG, "Client (%u) send changemap to iRODS\n", thread_number);
					if (irods_filesystem_event_processor_error::IRODS_ERROR == conn.send_change_map_to_irods(&inp)) {
                        LOG(LOG_DBG, "Client (%u) received error from iRODS\n", thread_number);
						irods_error_detected = true;
                        break;
					}
                    LOG(LOG_DBG, "Client (%u) changemap successfully sent to iRODS\n", thread_number);
				} else {
					irods_error_detected = true;
                    break;
				}

				if (irods_error_detected) {

					// irods was previous up but now is down

					// send message to changelog reader to pause reading changelog
					LOG(LOG_DBG, "irods client (%u): sending pause message to changelog_reader\n", thread_number);
					s_sendmore(publisher, "changelog_reader");
					std::string msg = str(boost::format("pause:%u") % thread_number);
					s_send(publisher, msg.c_str());

					// update the status to fail and send to accumulator
                    boost::shared_ptr< std::vector<uint8_t>> new_buffer;
					set_update_status_in_avro_buf(buffer, "FAIL", new_buffer);
					zmq::message_t response_message(new_buffer->size());
					memcpy(static_cast<char*>(response_message.data()), new_buffer->data(), new_buffer->size());
					sender.send(response_message);
				} else {
					// update the status to pass and send to accumulator
                    boost::shared_ptr< std::vector<uint8_t>> new_buffer;
					LOG(LOG_DBG, "irods client (%u): calling set_update_status_in_avro_buf\n", thread_number);
					set_update_status_in_avro_buf(buffer, "PASS", new_buffer);
					LOG(LOG_DBG, "irods client (%u): done calling set_update_status_in_avro_buf\n", thread_number);
					zmq::message_t response_message(new_buffer->size());
					memcpy(static_cast<char*>(response_message.data()), new_buffer->data(), new_buffer->size());
					LOG(LOG_DBG, "irods client (%u): sending message to accumulator\n", thread_number);
					sender.send(response_message);
			   }

			}  
        }
        
        if (irods_error_detected) {
    
            // in a failure state, remain here until we have detected that iRODS is back up

            // try a connection in a loop until irods is back up. 
            do {

                // initiate a connection object
                irods_connection conn(thread_number);


                // sleep for sleep_period in a 1s loop so we can catch a terminate message
                for (unsigned int i = 0; i < config_struct_ptr->irods_client_connect_failure_retry_seconds; ++i) {
                    sleep(1);

                    // see if there is a quit message, if so terminate
                    if (received_terminate_message(subscriber)) {
                        LOG(LOG_DBG, "irods client (%u) received a terminate message\n", thread_number);
                        LOG(LOG_DBG,"irods client (%u) exiting\n", thread_number);
                        return;
                    }
                }

                // double sleep period
                //sleep_period = sleep_period << 1;

            } while (0 != conn.instantiate_irods_connection(config_struct_ptr, thread_number )); 
            
            // irods is back up, set status and send a message to the changelog reader
            
            irods_error_detected = false;
            LOG(LOG_DBG, "sending continue message to changelog reader\n");
            std::string msg = str(boost::format("continue:%u") % thread_number);
            s_sendmore(publisher, "changelog_reader");
            s_send(publisher, msg.c_str());

            // sleep for sleep_period in a 1s loop so we can catch a terminate message
            for (unsigned int i = 0; i < config_struct_ptr->irods_client_connect_failure_retry_seconds; ++i) {
                sleep(1);
            }
        }


        // see if there is a quit message, if so terminate
        if (received_terminate_message(subscriber)) {
             LOG(LOG_DBG, "irods client (%u) received a terminate message\n", thread_number);
             quit = true;
             break;
        }

        // TODO figure the best way to do this
        sleep(5);

    }

    LOG(LOG_DBG,"irods client (%u) exiting\n", thread_number);
}


int main(int argc, char *argv[]) {

    std::string config_file = "filesystem_event_aggregator_config.json";
    std::string log_file;
    bool fatal_error_detected = false;

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

    filesystem_event_aggregator_cfg_t config_struct;
    rc = read_config_file(config_file, &config_struct);
    if (rc < 0) {
        return EX_CONFIG;
    }

    LOG(LOG_DBG, "initializing change_map serialized database\n");
    if (initiate_change_map_serialization_database("filesystem_event_aggregator") < 0) {
        LOG(LOG_ERR, "failed to initialize serialization database\n");
        return EX_SOFTWARE;
    }

    // create the changemap in memory and read from serialized DB
    change_map_t change_map;

    LOG(LOG_DBG, "reading change_map from serialized database\n");
    if (deserialize_change_map_from_sqlite(change_map, "filesystem_event_aggregator") < 0) {
        LOG(LOG_ERR, "failed to deserialize change map on startup\n");
        return EX_SOFTWARE;
    }

    print_change_table(change_map);

    // connect to irods and get the resource id from the resource name 
    // uses irods environment for this initial connection
    { 
        irods_connection conn(0);

        rc = conn.instantiate_irods_connection(nullptr, 0); 
        if (rc < 0) {
            LOG(LOG_ERR, "instantiate_irods_connection failed.  exiting...\n");
            return EX_SOFTWARE;
        }

        // read the resource id from resource name
        rc = conn.populate_irods_resc_id(&config_struct); 
        if (rc < 0) {
            LOG(LOG_ERR, "populate_irods_resc_id returned an error\n");
            return EX_SOFTWARE;
        }
    }

    // create a std::set of objectIdentifier which is used to pause sending updates to irods client updater threads
    // when a dependency is detected 
    std::set<std::string> active_objectIdentifier_list;


    // start a pub/sub publisher which is used to terminate threads and to send irods up/down messages
    zmq::context_t context(1);
    zmq::socket_t publisher(context, ZMQ_PUB);
    LOG(LOG_DBG, "main publisher conn_str = %s\n", config_struct.irods_client_broadcast_address.c_str());
    publisher.bind(config_struct.irods_client_broadcast_address);

    // start another pub/sub which is used for clients to send a stop reading
    // events message if iRODS is down
    /*zmq::socket_t subscriber(context, ZMQ_SUB);
    LOG(LOG_DBG, "main subscriber conn_str = %s\n", config_struct.changelog_reader_broadcast_address.c_str());
    subscriber.bind(config_struct.changelog_reader_broadcast_address);
    std::string identity("changelog_reader");
    subscriber.setsockopt(ZMQ_SUBSCRIBE, identity.c_str(), identity.length());*/

    // start accumulator thread which receives results back from iRODS updater threads
    std::thread accumulator_thread(result_accumulator_main, &config_struct, &change_map, &active_objectIdentifier_list); 

    // create a vector of irods client updater threads 
    std::vector<std::thread> irods_api_client_thread_list;

    // start up the threads
    for (unsigned int i = 0; i < config_struct.irods_updater_thread_count; ++i) {
        std::thread t(irods_api_client_main, &config_struct, &change_map, i, &active_objectIdentifier_list);
        irods_api_client_thread_list.push_back(std::move(t));
    }

    // main event aggregator loop, receive messages from readers and add to change log table
    zmq::context_t context2(1);
    zmq::socket_t socket (context2, ZMQ_REP);
    socket.bind (config_struct.event_aggregator_address);

    unsigned long long last_cr_index = 0;
    while (keep_running.load()) {

        zmq::message_t request;

        //  Wait for next request from client
        try {
            socket.recv(&request);
        } catch (const zmq::error_t& e) {
            continue;
        }


        std::auto_ptr<avro::InputStream> in = avro::memoryInputStream( 
                static_cast<const uint8_t*>(request.data()), request.size() );
        avro::DecoderPtr dec = avro::binaryDecoder();
        dec->init(*in);
        fs_event::filesystem_event event; 
        avro::decode(*dec, event);

        printf("Received event: [%zu, %s, %s, %s, %s, %s, %s, %s]\n", event.index, event.event_type.c_str(), event.root_path.c_str(),
                event.entryId.c_str(), event.targetParentId.c_str(), event.basename.c_str(), event.full_target_path.c_str(), event.full_path.c_str());

        size_t change_table_size = get_change_table_size(change_map);
        LOG(LOG_DBG, " change_table size is %zu\n", change_table_size);

        if (change_table_size > config_struct.maximum_queued_records) {

            // Reached max number of records we will process, do not queue message
            // and send a pause message to reader.  Reader responsible to resend.
            zmq::message_t reply(5);
            memcpy(reply.data(), "PAUSE", 5);
            socket.send(reply);
        } else {

            // write entry to change_map
            if (event.event_type == "CREATE") {
                handle_create(event.index, event.root_path, event.entryId, event.targetParentId, event.basename, event.full_path, change_map);
            } else if (event.event_type == "CLOSE") {
                handle_close(event.index, event.root_path, event.entryId, event.targetParentId, event.basename, event.full_path, change_map);
            } else if (event.event_type == "UNLINK") {
                handle_unlink(event.index, event.root_path, event.entryId, event.targetParentId, event.basename, event.full_path, change_map);
            } else if (event.event_type == "MKDIR") {
                handle_mkdir(event.index, event.root_path, event.entryId, event.targetParentId, event.basename, event.full_path, change_map);
            } else if (event.event_type == "RMDIR") {
                handle_rmdir(event.index, event.root_path, event.entryId, event.targetParentId, event.basename, event.full_path, change_map);
            } else if (event.event_type == "RENAME") {
                handle_rename(event.index, event.root_path, event.entryId, event.targetParentId, event.basename, event.full_target_path, event.full_path, change_map);
            } else if (event.event_type == "TRUNCATE") {
                handle_trunc(event.index, event.root_path, event.entryId, event.targetParentId, event.basename, event.full_path, change_map);
            } else {
                LOG(LOG_ERR, "Unknown event type (%s) received from listener.  Skipping...\n", event.event_type.c_str());
            }

            // reply CONTNUE to inform the reader to continue reading messages 
            zmq::message_t reply (8);
            memcpy (reply.data (), "CONTINUE", 8); 
            socket.send (reply);
        }
    }

    // send message to threads to terminate
    LOG(LOG_DBG, "sending terminate message to clients\n");
    s_sendmore(publisher, "changetable_readers");
    s_send(publisher, "terminate"); 

    //irods_api_client_thread.join();
    for (auto iter = irods_api_client_thread_list.begin(); iter != irods_api_client_thread_list.end(); ++iter) {
        iter->join();
    }

    accumulator_thread.join();

    LOG(LOG_DBG, "serializing change_map to database\n");
    if (serialize_change_map_to_sqlite(change_map, "filesystem_event_aggregator") < 0) {
        LOG(LOG_ERR, "failed to serialize change_map upon exit\n");
        fatal_error_detected = true;
    }

    if (write_cr_index_to_sqlite(last_cr_index, "filesystem_event_aggregator") < 0) {
        LOG(LOG_ERR, "failed to write cr_index to database upon exit\n");
        fatal_error_detected = true;
    }

    LOG(LOG_DBG,"changelog client exiting\n");
    if (stdout != dbgstream) {
        fclose(dbgstream);
    }

    if (fatal_error_detected) {
        return EX_SOFTWARE;
    }

    return EX_OK;
}
