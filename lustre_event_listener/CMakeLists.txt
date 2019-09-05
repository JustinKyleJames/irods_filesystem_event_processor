cmake_minimum_required(VERSION 3.5.0 FATAL_ERROR) 

find_package(IRODS 4.2.6 EXACT REQUIRED)

set(CMAKE_C_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang)
set(CMAKE_CXX_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang++)
set (CMAKE_CXX_STANDARD 14)

project(lustre-event-listener C CXX)

find_package(Threads REQUIRED)

include(${IRODS_TARGETS_PATH})

if (NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build {Debug, Release}." FORCE)
  message(STATUS "Setting unspecified CMAKE_BUILD_TYPE to '${CMAKE_BUILD_TYPE}'")
endif()

if (NOT CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/" CACHE STRING "Install root path. Change from \"/\" for run-in-place installations." FORCE)
  message(STATUS "Setting unspecified CMAKE_INSTALL_PREFIX to '${CMAKE_INSTALL_PREFIX}'. This is the correct setting for normal builds.")
endif()

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_INSTALL_RPATH ${IRODS_EXTERNALS_FULLPATH_CLANG_RUNTIME}/lib)


link_directories(${IRODS_EXTERNALS_FULLPATH_CLANG_RUNTIME}/lib /usr/local/lib)

message("IRODS_EXTERNALS_FULLPATH_CLANG_RUNTIME is ${IRODS_EXTERNALS_FULLPATH_CLANG_RUNTIME}")
message("CMAKE_INSTALL_RPATH is ${CMAKE_INSTALL_RPATH}")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++")
set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -stdlib=libc++")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -stdlib=libc++")
add_compile_options(-nostdinc++ -Wall -Wextra -Werror -Wno-unused-parameter)


link_libraries(c++abi
    pthread
    dl
    rt
    kj
    zmq
    lustreapi
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_program_options.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_thread.so
    ${IRODS_EXTERNALS_FULLPATH_ZMQ}/lib/libzmq.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so
    ${IRODS_EXTERNALS_FULLPATH_AVRO}/lib/libavrocpp.so)

include_directories(/usr/include/irods
    ${IRODS_EXTERNALS_FULLPATH_CLANG}/include/c++/v1 
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
    ${CMAKE_SOURCE_DIR}/src/
    ${CMAKE_SOURCE_DIR}/../common
    ${IRODS_EXTERNALS_FULLPATH_CPPZMQ}/include
    ${IRODS_EXTERNALS_FULLPATH_JANSSON}/include
    ${IRODS_EXTERNALS_FULLPATH_AVRO}/include)

set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -Wl,-z,defs")

add_executable(lustre_event_listener 
    ${PROJECT_SOURCE_DIR}/../common/file_system_event_avro.hpp
    ${PROJECT_SOURCE_DIR}/../common/file_system_event_avro_schema.json
    ${PROJECT_SOURCE_DIR}/src/config.cpp 
    ${PROJECT_SOURCE_DIR}/src/main.cpp
    ${PROJECT_SOURCE_DIR}/src/llapi_cpp_wrapper.c)

set_target_properties(lustre_event_listener PROPERTIES LINKER_LANGUAGE CXX)

add_custom_command(TARGET lustre_event_listener PRE_BUILD
                       COMMAND ${CMAKE_COMMAND} -E copy
                       ${CMAKE_SOURCE_DIR}/src/lustre_event_listener_config.json ${CMAKE_CURRENT_BINARY_DIR}/lustre_event_listener_config.json)

add_custom_command(OUTPUT ${PROJECT_SOURCE_DIR}/../common/file_system_event_avro.hpp
    COMMAND ${IRODS_EXTERNALS_FULLPATH_AVRO}/bin/avrogencpp -i ${PROJECT_SOURCE_DIR}/../common/file_system_event_avro_schema.json -o ${PROJECT_SOURCE_DIR}/../common/file_system_event_avro.hpp -n fs_event)
