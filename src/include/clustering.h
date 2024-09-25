/*
 * Copyright (C) 2024 The pgmoneta community
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or other
 * materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors may
 * be used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef PGMONETA_CLUSTERING_H
#define PGMONETA_CLUSTERING_H

#ifdef __cplusplus
extern "C" {
#endif

/* pgmoneta */
#include <pgmoneta.h>
#include <deque.h>
#include <json.h>

#include <stdbool.h>
#include <stdlib.h>

#include <openssl/ssl.h>

#define CLUSTERING_UNKNOWN -1
#define CLUSTERING_NO       0
#define CLUSTERING_YES      1

/**
 * Clustering header
 */
#define CLUSTERING_COMPRESSION_NONE 0

#define CLUSTERING_ENCRYPTION_NONE 0

/**
 * Clustering commands
 */
#define CLUSTERING_HAS_CHANGES     1
#define CLUSTERING_GET_ID          2
#define CLUSTERING_GET_SERVERS     3
#define CLUSTERING_GET_BACKUPS     4

/**
 * Clustering categories
 */
#define CLUSTERING_CATEGORY_HEADER   "Header"
#define CLUSTERING_CATEGORY_REQUEST  "Request"
#define CLUSTERING_CATEGORY_RESPONSE "Response"
#define CLUSTERING_CATEGORY_OUTCOME  "Outcome"

/**
 * Clustering arguments
 */
#define CLUSTERING_ARGUMENT_ID                    "Id"
#define CLUSTERING_ARGUMENT_BACKUP                "Backup"
#define CLUSTERING_ARGUMENT_BACKUPS               "Backups"
#define CLUSTERING_ARGUMENT_COMMAND               "Command"
#define CLUSTERING_ARGUMENT_ERROR                 "Error"
#define CLUSTERING_ARGUMENT_HOST                  "Host"
#define CLUSTERING_ARGUMENT_NUMBER_OF_SERVERS     "NumberOfServers"
#define CLUSTERING_ARGUMENT_SERVER                "Server"
#define CLUSTERING_ARGUMENT_SERVERS               "Servers"
#define CLUSTERING_ARGUMENT_STATUS                "Status"
#define CLUSTERING_ARGUMENT_TIME                  "Time"
#define CLUSTERING_ARGUMENT_TIMESTAMP             "Timestamp"
#define CLUSTERING_ARGUMENT_VERSION               "Version"

/**
 * Clustering error
 */
#define CLUSTERING_ERROR_BAD_PAYLOAD     10001
#define CLUSTERING_ERROR_UNKNOWN_COMMAND 10002
#define CLUSTERING_ERROR_ALLOCATION      10003

#define CLUSTERING_ERROR_HAS_CHANGES_NETWORK  10101
#define CLUSTERING_ERROR_HAS_CHANGES_NOFORK   10102
#define CLUSTERING_ERROR_HAS_CHANGES_ERROR    10103

#define CLUSTERING_ERROR_GET_ID_NETWORK  10201
#define CLUSTERING_ERROR_GET_ID_NOFORK   10202
#define CLUSTERING_ERROR_GET_ID_ERROR    10203

#define CLUSTERING_ERROR_GET_SERVERS_NETWORK 10301
#define CLUSTERING_ERROR_GET_SERVERS_NOFORK  10302
#define CLUSTERING_ERROR_GET_SERVERS_ERROR   10303

/* #define BACKUP_IDENTFIER_SIZE   15 */
/* #define FILE_SHA                65 */
/* #define BLOCK_SIZE            8192 */

/* #define REQUEST_GET_NUMBER_OF_BACKUPS   1 */
/* #define RESPONSE_GET_NUMBER_OF_BACKUPS  2 */
/* #define REQUEST_GET_BACKUP              3 */
/* #define RESPONSE_GET_BACKUP             4 */
/* #define REQUEST_GET_NUMBER_OF_FILES     5 */
/* #define RESPONSE_GET_NUMBER_OF_FILES    6 */
/* #define REQUEST_GET_FILE_NAME           7 */
/* #define RESPONSE_GET_FILE_NAME          8 */
/* #define REQUEST_GET_BLOCK               9 */
/* #define RESPONSE_GET_BLOCK             10 */

/* /\** @struct clustering_get_number_of_backups_request */
/*  * Defines the get number of backups request */
/*  *\/ */
/* struct clustering_get_number_of_backups_request */
/* { */
/*    struct clustering_base_request base; /\**< The base *\/ */
/* }; */

/* /\** @struct clustering_get_number_of_backups_response */
/*  * Defines the get number of backups response */
/*  *\/ */
/* struct clustering_get_number_of_backups_response */
/* { */
/*    struct clustering_base_response base; /\**< The base *\/ */
/*    unsigned int number;                  /\**< The number of backups *\/ */
/* }; */

/* /\** @struct clustering_get_backup_request */
/*  * Defines the get backup request */
/*  *\/ */
/* struct clustering_get_backup_request */
/* { */
/*    struct clustering_base_request base; /\**< The base *\/ */
/*    unsigned int number;                 /\**< The backup number *\/ */
/* }; */

/* /\** @struct clustering_get_backup_response */
/*  * Defines the get backup response */
/*  *\/ */
/* struct clustering_get_backup_response */
/* { */
/*    struct clustering_base_response base; /\**< The base *\/ */
/*    unsigned int number;                  /\**< The backup number *\/ */
/*    char backup[BACKUP_IDENTFIER_SIZE];   /\**< The name of the backup *\/ */
/* }; */

/* /\** @struct clustering_get_number_of_files_request */
/*  * Defines the get number of files request */
/*  *\/ */
/* struct clustering_get_number_of_files_request */
/* { */
/*    struct clustering_base_request base; /\**< The base *\/ */
/*    char backup[BACKUP_IDENTFIER_SIZE];  /\**< The name of the backup *\/ */
/* }; */

/* /\** @struct clustering_get_number_of_files_response */
/*  * Defines the get number of files response */
/*  *\/ */
/* struct clustering_get_number_of_files_response */
/* { */
/*    struct clustering_base_response base; /\**< The base *\/ */
/*    char backup[BACKUP_IDENTFIER_SIZE];   /\**< The name of the backup *\/ */
/*    unsigned int number;                  /\**< The number of files *\/ */
/* }; */

/* /\** @struct clustering_get_file_name_request */
/*  * Defines the get file name request */
/*  *\/ */
/* struct clustering_get_file_name_request */
/* { */
/*    struct clustering_base_request base; /\**< The base *\/ */
/*    unsigned int number;                 /\**< The file number *\/ */
/* }; */

/* /\** @struct clustering_get_file_name_response */
/*  * Defines the get file response */
/*  *\/ */
/* struct clustering_get_file_name_response */
/* { */
/*    struct clustering_base_response base; /\**< The base *\/ */
/*    unsigned int number;                  /\**< The file number *\/ */
/*    char filename[MAX_PATH];              /\**< The file name *\/ */
/*    unsigned int filelength;              /\**< The file length *\/ */
/*    char filesha[FILE_SHA];               /\**< The file SHA *\/ */
/* }; */

/* /\** @struct clustering_get_block_request */
/*  * Defines the get block request */
/*  *\/ */
/* struct clustering_get_block_request */
/* { */
/*    struct clustering_base_request base; /\**< The base *\/ */
/*    unsigned int number;                 /\**< The file number *\/ */
/*    unsigned int block_number;           /\**< The block number *\/ */
/* }; */

/* /\** @struct clustering_get_block_response */
/*  * Defines the get file response */
/*  *\/ */
/* struct clustering_get_block_response */
/* { */
/*    struct clustering_base_response base; /\**< The base *\/ */
/*    unsigned int number;                  /\**< The file number *\/ */
/*    unsigned int block_number;            /\**< The block number *\/ */
/*    unsigned short block_length;          /\**< The block length *\/ */
/*    char block[BLOCK_SIZE];               /\**< The block data *\/ */
/* }; */

/**
 * Check which nodes are active
 * @return 0 upon success, otherwise 1
 */
int
pgmoneta_clustering_check_active(void);

/**
 * Clustering run
 * @param node The node
 * @param server The server
 */
void
pgmoneta_clustering_run(int32_t node, int32_t server);

/**
 * Handle clustering get id request
 * @param ssl The SSL connection
 * @param socket The client socket
 */
void
pgmoneta_clustering_request_get_id(SSL* ssl, int socket);

/**
 * Handle clustering get id response
 * @param ssl The SSL connection
 * @param socket The client socket
 * @param payload The payload
 */
void
pgmoneta_clustering_response_get_id(SSL* ssl, int socket, struct json* payload);

/**
 * Handle clustering get servers request
 * @param ssl The SSL connection
 * @param socket The client socket
 */
void
pgmoneta_clustering_request_get_servers(SSL* ssl, int socket);

/**
 * Handle clustering get servers response
 * @param ssl The SSL connection
 * @param socket The client socket
 * @param payload The payload
 */
void
pgmoneta_clustering_response_get_servers(SSL* ssl, int socket, struct json* payload);

/**
 * Handle clustering get backups request
 * @param ssl The SSL connection
 * @param socket The client socket
 * @param server The server
 */
void
pgmoneta_clustering_request_get_backups(SSL* ssl, int socket, char* server);

/**
 * Handle clustering get backups response
 * @param ssl The SSL connection
 * @param socket The client socket
 * @param payload The payload
 */
void
pgmoneta_clustering_response_get_backups(SSL* ssl, int socket, struct json* payload);

/**
 * Generate an ok response
 * @param payload The payload
 * @param start_time The start time
 * @param end_time The end time
 * @return 0 upon success, otherwise 1
 */
int
pgmoneta_clustering_response_ok(struct json* payload, time_t start_time, time_t end_time);

/**
 * Generate an error response
 * @param payload The payload
 * @param error The error code
 * @return 0 upon success, otherwise 1
 */
int
pgmoneta_clustering_response_error(struct json* payload, int32_t error);

/**
 * Read a JSON document
 * @param ssl The SSL connection
 * @param socket The client socket
 * @param json The resulting JSON document
 * @return 0 upon success, otherwise 1
 */
int
pgmoneta_clustering_read_json(SSL* ssl, int socket, struct json** json);

/**
 * Write a JSON document
 * @param ssl The SSL connection
 * @param socket The client socket
 * @param json The JSON document
 * @return 0 upon success, otherwise 1
 */
int
pgmoneta_clustering_write_json(SSL* ssl, int socket, struct json* json);

#ifdef __cplusplus
}
#endif

#endif
