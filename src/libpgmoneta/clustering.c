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

/* pgmoneta */
#include <pgmoneta.h>
#include <bitset.h>
#include <clustering.h>
#include <deque.h>
#include <logging.h>
#include <network.h>
#include <utils.h>

#include <string.h>

#include <openssl/err.h>
#include <openssl/ssl.h>


static int create_header(int32_t command, struct json** json);
static int create_request(struct json* json, struct json** request);
static int create_response(struct json* json, struct json** response);
static int create_outcome_success(struct json* json, time_t start_time, time_t end_time, struct json** outcome);
static int create_outcome_failure(struct json* json, int32_t error, struct json** outcome);

static bool is_defined_unique_id(char* id);
static bool is_defined_server(char* server);

static int read_uint8(char* prefix, SSL* ssl, int socket, uint8_t* i);
static int read_string(char* prefix, SSL* ssl, int socket, char** str);
static int write_uint8(char* prefix, SSL* ssl, int socket, uint8_t i);
static int write_string(char* prefix, SSL* ssl, int socket, char* str);
static int read_complete(SSL* ssl, int socket, void* buf, size_t size);
static int write_complete(SSL* ssl, int socket, void* buf, size_t size);
static int write_socket(int socket, void* buf, size_t size);
static int write_ssl(SSL* ssl, void* buf, size_t size);

int
pgmoneta_clustering_check_active(void)
{
   int non = 0;
   struct configuration* config;

   config = (struct configuration*)shmem;

   pgmoneta_log_trace("clustering: %d", config->clustering);
   pgmoneta_log_trace("clustering_id: %s", config->clustering_id);
   pgmoneta_log_trace("clustering_nodes: %s", config->clustering_nodes);

   for (int i = 0; i < NUMBER_OF_SERVERS; i++)
   {
      memset(&config->nodes[i], 0, sizeof(struct clustering_node));
   }

   config->number_of_nodes = 0;
   
   if (strlen(config->clustering_nodes) > 0)
   {
      char* nodes = NULL;
      char* ptr = NULL;

      nodes = pgmoneta_append(nodes, config->clustering_nodes);

      ptr = strtok(nodes, ",");

      while (ptr != NULL)
      {
         char* copied_ptr = NULL;
         char* h = NULL;
         char* p = NULL;

         copied_ptr = pgmoneta_append(copied_ptr, ptr);

         ptr = strtok(NULL, ",");

         h = strtok(copied_ptr, ":");
         p = strtok(NULL, ":");

         memcpy(config->nodes[non].host, h, strlen(h));
         config->nodes[non].port = atoi(p);

         non++;

         free(copied_ptr);
      }

      free(nodes);
   }

   config->number_of_nodes = non;

   for (int i = 0; i < config->number_of_nodes; i++)
   {
      int socket = 0;
      char* host = NULL;
      char* id = NULL;
      struct json* payload = NULL;
      struct json* response = NULL;

      config->nodes[i].active = false;

      if (pgmoneta_connect(config->nodes[i].host, config->nodes[i].port, &socket))
      {
         pgmoneta_log_warn("Clustering: No connection to %s:%d", config->nodes[i].host, config->nodes[i].port);
         continue;
      }

      if (pgmoneta_socket_buffers(socket))
      {
         pgmoneta_log_warn("Clustering: Could not set buffers on %s:%d", config->nodes[i].host, config->nodes[i].port);
         goto done_get_id;
      }

      pgmoneta_log_debug("Clustering host: %s:%d", config->nodes[i].host, config->nodes[i].port);
      
      pgmoneta_clustering_request_get_id(NULL, socket);

      if (pgmoneta_clustering_read_json(NULL, socket, &payload))
      {
         goto done_get_id;
      }

      response = (struct json*)pgmoneta_json_get(payload, CLUSTERING_CATEGORY_RESPONSE);

      host = (char*)pgmoneta_json_get(response, CLUSTERING_ARGUMENT_HOST);
      if (strcmp("*", host))
      {
         id = (char*)pgmoneta_json_get(response, CLUSTERING_ARGUMENT_ID);

         if (!is_defined_unique_id(id))
         {
            memcpy(config->nodes[i].id, id, strlen(id));
            config->nodes[i].active = true;
         }
         else
         {
            pgmoneta_log_warn("Clustering: Unique identifier %s is already defined", id);
         }
      }
      else
      {
         pgmoneta_log_warn("Clustering: Host name need a specific interface for %s:%d", config->nodes[i].host, config->nodes[i].port);
      }

done_get_id:

      pgmoneta_json_destroy(payload);
      pgmoneta_disconnect(socket);
   }

   for (int i = 0; i < config->number_of_nodes; i++)
   {
      int socket = 0;
      int nos = 0;
      char* s_name = NULL;
      struct json* payload = NULL;
      struct json* response = NULL;
      struct json* servers = NULL;
      struct json_iterator* siter = NULL;

      for (int j = 0; j < NUMBER_OF_SERVERS; j++)
      {
         memset(&config->nodes[i].server_names[j], 0, MISC_LENGTH);
      }

      config->nodes[i].number_of_servers = 0;

      if (config->nodes[i].active)
      {
         if (pgmoneta_connect(config->nodes[i].host, config->nodes[i].port, &socket))
         {
            pgmoneta_log_warn("Clustering: No connection to %s:%d", config->nodes[i].host, config->nodes[i].port);
            continue;
         }

         if (pgmoneta_socket_buffers(socket))
         {
            pgmoneta_log_warn("Clustering: Could not set buffers on %s:%d", config->nodes[i].host, config->nodes[i].port);
            goto done_get_servers;
         }

         pgmoneta_log_debug("Clustering host: %s:%d", config->nodes[i].host, config->nodes[i].port);
      
         pgmoneta_clustering_request_get_servers(NULL, socket);

         if (pgmoneta_clustering_read_json(NULL, socket, &payload))
         {
            goto done_get_servers;
         }

         response = (struct json*)pgmoneta_json_get(payload, CLUSTERING_CATEGORY_RESPONSE);
         servers = (struct json*)pgmoneta_json_get(response, CLUSTERING_ARGUMENT_SERVERS);

         if (pgmoneta_json_iterator_create(servers, &siter))
         {
            goto done_get_servers;
         }

         while (pgmoneta_json_iterator_next(siter))
         {
            struct json* s = NULL;

            s = (struct json*)pgmoneta_value_data(siter->value);

            s_name = (char*)pgmoneta_json_get(s, CLUSTERING_ARGUMENT_SERVER);

            if (!is_defined_server(s_name))
            {
               memcpy(config->nodes[i].server_names[nos], s_name, strlen(s_name));

               pgmoneta_log_info("Clustering: %s -> %s", config->nodes[i].id, s_name);

               nos++;
            }
            else
            {
               pgmoneta_log_warn("Clustering: Server %s is already defined", s_name);
            }
         }

         config->nodes[i].number_of_servers = nos;

         pgmoneta_json_iterator_destroy(siter);
      }

done_get_servers:

      pgmoneta_json_destroy(payload);
      pgmoneta_disconnect(socket);
   }

   return 0;
}

void
pgmoneta_clustering_run(int32_t node, int32_t server)
{
   int socket = -1;
   struct configuration* config;

   config = (struct configuration*)shmem;

   pgmoneta_log_debug("Clustering: %s/%s", config->nodes[node].id, config->nodes[node].server_names[server]);

   if (pgmoneta_connect(config->nodes[node].host, config->nodes[node].port, &socket))
   {
      pgmoneta_log_error("Clustering: No connection to %s:%d", config->nodes[node].host, config->nodes[node].port);
      goto error;
   }

   if (pgmoneta_socket_buffers(socket))
   {
      pgmoneta_log_warn("Clustering: Could not set buffers on %s:%d", config->nodes[node].host, config->nodes[node].port);
      goto error;
   }

   // TODO

   pgmoneta_disconnect(socket);

error:

   pgmoneta_disconnect(socket);
}

void
pgmoneta_clustering_request_get_id(SSL* ssl, int socket)
{
   struct json* j = NULL;
   struct json* request = NULL;

   if (create_header(CLUSTERING_GET_ID, &j))
   {
      goto error;
   }

   if (create_request(j, &request))
   {
      goto error;
   }

   if (pgmoneta_clustering_write_json(ssl, socket, j))
   {
      goto error;
   }

   pgmoneta_json_destroy(j);

   return;

error:

   pgmoneta_json_destroy(j);
}

void
pgmoneta_clustering_response_get_id(SSL* ssl, int socket, struct json* payload)
{
   time_t t;
   char timestamp[128];
   struct tm* time_info;
   time_t start_time;
   time_t end_time;
   struct json* j = NULL;
   struct json* header = NULL;
   struct json* response = NULL;
   struct configuration* config;

   pgmoneta_log_info("pgmoneta_clustering_response_get_id: %d", socket);

   config = (struct configuration*)shmem;

   start_time = time(NULL);

   if (pgmoneta_json_clone(payload, &j))
   {
      goto  error;
   }

   time(&t);
   time_info = localtime(&t);
   strftime(&timestamp[0], sizeof(timestamp), "%Y%m%d%H%M%S", time_info);

   header = (struct json*)pgmoneta_json_get(j, CLUSTERING_CATEGORY_HEADER);
   pgmoneta_json_put(header, CLUSTERING_ARGUMENT_ID, (uintptr_t)config->clustering_id, ValueString);
   pgmoneta_json_put(header, CLUSTERING_ARGUMENT_TIMESTAMP, (uintptr_t)timestamp, ValueString);

   if (create_response(j, &response))
   {
      goto error;
   }

   pgmoneta_json_put(response, CLUSTERING_ARGUMENT_HOST, (uintptr_t)config->host, ValueString);

   end_time = time(NULL);

   if (pgmoneta_clustering_response_ok(j, start_time, end_time))
   {
      pgmoneta_clustering_response_error(j, CLUSTERING_ERROR_GET_ID_NETWORK);
      pgmoneta_clustering_write_json(ssl, socket, j);
      goto error;
   }
   
   if (pgmoneta_clustering_write_json(ssl, socket, j))
   {
      pgmoneta_clustering_response_error(j, CLUSTERING_ERROR_GET_ID_NETWORK);
      pgmoneta_clustering_write_json(ssl, socket, j);
      goto error;
   }

   pgmoneta_json_destroy(j);

   return;

error:

   pgmoneta_json_destroy(j);
}

void
pgmoneta_clustering_request_get_servers(SSL* ssl, int socket)
{
   struct json* j = NULL;
   struct json* request = NULL;

   if (create_header(CLUSTERING_GET_SERVERS, &j))
   {
      goto error;
   }

   if (create_request(j, &request))
   {
      goto error;
   }

   if (pgmoneta_clustering_write_json(ssl, socket, j))
   {
      goto error;
   }

   pgmoneta_json_destroy(j);

   return;

error:

   pgmoneta_json_destroy(j);
}

void
pgmoneta_clustering_response_get_servers(SSL* ssl, int socket, struct json* payload)
{
   int nos = 0;
   time_t t;
   char timestamp[128];
   struct tm* time_info;
   struct json* j = NULL;
   struct json* header = NULL;
   struct json* response = NULL;
   struct json* servers = NULL;
   struct configuration* config;

   config = (struct configuration*)shmem;

   if (pgmoneta_json_clone(payload, &j))
   {
      goto  error;
   }

   time(&t);
   time_info = localtime(&t);
   strftime(&timestamp[0], sizeof(timestamp), "%Y%m%d%H%M%S", time_info);

   header = (struct json*)pgmoneta_json_get(j, CLUSTERING_CATEGORY_HEADER);
   pgmoneta_json_put(header, CLUSTERING_ARGUMENT_ID, (uintptr_t)config->clustering_id, ValueString);
   pgmoneta_json_put(header, CLUSTERING_ARGUMENT_TIMESTAMP, (uintptr_t)timestamp, ValueString);

   if (create_response(j, &response))
   {
      goto error;
   }

   if (pgmoneta_json_create(&servers))
   {
      goto  error;
   }

   for (int i = 0; i < config->number_of_servers; i++)
   {
      struct json* srv = NULL;

      if (config->servers[i].clustering)
      {
         if (pgmoneta_json_create(&srv))
         {
            goto  error;
         }

         pgmoneta_json_put(srv, CLUSTERING_ARGUMENT_SERVER, (uintptr_t)config->servers[i].name, ValueString);

         pgmoneta_json_append(servers, (uintptr_t)srv, ValueJSON);

         nos++;
      }
   }

   pgmoneta_json_put(response, CLUSTERING_ARGUMENT_SERVERS, (uintptr_t)servers, ValueJSON);

   pgmoneta_json_put(response, CLUSTERING_ARGUMENT_NUMBER_OF_SERVERS, (uintptr_t)nos, ValueInt32);

   if (pgmoneta_clustering_write_json(ssl, socket, j))
   {
      goto error;
   }

   pgmoneta_json_destroy(j);

   return;

error:

   pgmoneta_json_destroy(j);
}

void
pgmoneta_clustering_request_get_backups(SSL* ssl, int socket, char* server)
{
   struct json* j = NULL;
   struct json* request = NULL;

   if (create_header(CLUSTERING_GET_BACKUPS, &j))
   {
      goto error;
   }

   if (create_request(j, &request))
   {
      goto error;
   }

   pgmoneta_json_put(request, CLUSTERING_ARGUMENT_SERVER, (uintptr_t)server, ValueString);

   if (pgmoneta_clustering_write_json(ssl, socket, j))
   {
      goto error;
   }

   pgmoneta_json_destroy(j);

   return;

error:

   pgmoneta_json_destroy(j);
}

void
pgmoneta_clustering_response_get_backups(SSL* ssl, int socket, struct json* payload)
{
   char* server = NULL;
   int srv = -1;
   struct json* j = NULL;
   struct json* request = NULL;
   struct json* response = NULL;
   struct json* bcks = NULL;
   char* d = NULL;
   int number_of_backups = 0;
   struct backup** backups = NULL;
   struct configuration* config;

   config = (struct configuration*)shmem;

   request = (struct json*)pgmoneta_json_get(payload, CLUSTERING_CATEGORY_REQUEST);
   server = (char*)pgmoneta_json_get(request, CLUSTERING_ARGUMENT_SERVER);

   for (int i = 0; srv == -1 && i < config->number_of_servers; i++)
   {
      if (!strcmp(config->servers[i].name, server))
      {
         srv = i;
      }
   }

   if (srv == -1)
   {
      goto error;
   }

   if (pgmoneta_json_clone(payload, &j))
   {
      goto  error;
   }

   if (create_response(j, &response))
   {
      goto error;
   }

   d = pgmoneta_get_server_backup(srv);

   if (pgmoneta_get_backups(d, &number_of_backups, &backups))
   {
      goto error;
   }

   free(d);
   d = NULL;

   if (pgmoneta_json_create(&bcks))
   {
      goto  error;
   }

   for (int i = 0; i < number_of_backups; i++)
   {
      struct json* bck = NULL;

      if (pgmoneta_json_create(&bck))
      {
         goto  error;
      }

      pgmoneta_json_put(bck, CLUSTERING_ARGUMENT_BACKUP, (uintptr_t)backups[i]->label, ValueString);

      pgmoneta_json_append(bcks, (uintptr_t)bck, ValueJSON);
   }

   pgmoneta_json_put(response, CLUSTERING_ARGUMENT_BACKUPS, (uintptr_t)bcks, ValueJSON);

   if (pgmoneta_clustering_write_json(ssl, socket, j))
   {
      goto error;
   }

   for (int i = 0; i < number_of_backups; i++)
   {
      free(backups[i]);
   }
   free(backups);

   pgmoneta_json_destroy(j);

   return;

error:

   for (int i = 0; i < number_of_backups; i++)
   {
      free(backups[i]);
   }
   free(backups);

   pgmoneta_json_destroy(j);
}

int
pgmoneta_clustering_response_ok(struct json* payload, time_t start_time, time_t end_time)
{
   struct json* outcome = NULL;

   if (create_outcome_success(payload, start_time, end_time, &outcome))
   {
      goto error;
   }

   return 0;

error:

   return 1;
}

int
pgmoneta_clustering_response_error(struct json* payload, int32_t error)
{
   struct json* outcome = NULL;

   if (create_outcome_failure(payload, error, &outcome))
   {
      goto error;
   }

   return 0;

error:

   return 1;
}

int
pgmoneta_clustering_read_json(SSL* ssl, int socket, struct json** json)
{
   uint8_t compression = CLUSTERING_COMPRESSION_NONE;
   uint8_t encryption = CLUSTERING_ENCRYPTION_NONE;
   char* s = NULL;
   struct json* r = NULL;

   if (read_uint8("pgmoneta-clustering", ssl, socket, &compression))
   {
      goto error;
   }

   if (read_uint8("pgmoneta-clustering", ssl, socket, &encryption))
   {
      goto error;
   }

   if (read_string("pgmoneta-clustering", ssl, socket, &s))
   {
      goto error;
   }

   pgmoneta_log_info("pgmoneta_read_json: %s", s);

   if (pgmoneta_json_parse_string(s, &r))
   {
      goto error;
   }

   *json = r;

   free(s);

   return 0;

error:

   pgmoneta_json_destroy(r);

   free(s);

   return 1;
}

int
pgmoneta_clustering_write_json(SSL* ssl, int socket, struct json* json)
{
   uint8_t compression = CLUSTERING_COMPRESSION_NONE;
   uint8_t encryption = CLUSTERING_ENCRYPTION_NONE;
   char* s = NULL;

   s = pgmoneta_json_to_string(json, FORMAT_JSON_COMPACT, NULL, 0);

   pgmoneta_log_info("pgmoneta_write_json: %s", s);

   if (write_uint8("pgmoneta-clustering", ssl, socket, compression))
   {
      goto error;
   }

   if (write_uint8("pgmoneta-clustering", ssl, socket, encryption))
   {
      goto error;
   }

   if (write_string("pgmoneta-clustering", ssl, socket, s))
   {
      goto error;
   }

   free(s);

   return 0;

error:

   free(s);

   return 1;
}

static int
create_header(int32_t command, struct json** json)
{
   time_t t;
   char timestamp[128];
   struct tm* time_info;
   struct json* j = NULL;
   struct json* header = NULL;
   struct configuration* config;

   config = (struct configuration*)shmem;

   *json = NULL;

   if (pgmoneta_json_create(&j))
   {
      goto error;
   }

   if (pgmoneta_json_create(&header))
   {
      goto error;
   }

   time(&t);
   time_info = localtime(&t);
   strftime(&timestamp[0], sizeof(timestamp), "%Y%m%d%H%M%S", time_info);

   pgmoneta_json_put(header, CLUSTERING_ARGUMENT_COMMAND, (uintptr_t)command, ValueInt32);
   pgmoneta_json_put(header, CLUSTERING_ARGUMENT_ID, (uintptr_t)config->clustering_id, ValueString);
   pgmoneta_json_put(header, CLUSTERING_ARGUMENT_VERSION, (uintptr_t)VERSION, ValueString);
   pgmoneta_json_put(header, CLUSTERING_ARGUMENT_TIMESTAMP, (uintptr_t)timestamp, ValueString);

   pgmoneta_json_put(j, CLUSTERING_CATEGORY_HEADER, (uintptr_t)header, ValueJSON);

   *json = j;

   return 0;

error:

   pgmoneta_json_destroy(header);
   pgmoneta_json_destroy(j);

   *json = NULL;

   return 1;
}

static int
create_request(struct json* json, struct json** request)
{
   struct json* r = NULL;

   *request = NULL;

   if (pgmoneta_json_create(&r))
   {
      goto error;
   }

   pgmoneta_json_put(json, CLUSTERING_CATEGORY_REQUEST, (uintptr_t)r, ValueJSON);

   *request = r;

   return 0;

error:

   pgmoneta_json_destroy(r);

   return 1;
}

static int
create_response(struct json* json, struct json** response)
{
   struct json* r = NULL;
   struct configuration* config;

   config = (struct configuration*)shmem;

   *response = NULL;

   if (pgmoneta_json_create(&r))
   {
      goto error;
   }

   pgmoneta_json_put(json, CLUSTERING_CATEGORY_RESPONSE, (uintptr_t)r, ValueJSON);

   pgmoneta_json_put(r, CLUSTERING_ARGUMENT_ID, (uintptr_t)config->clustering_id, ValueString);
   pgmoneta_json_put(r, CLUSTERING_ARGUMENT_VERSION, (uintptr_t)VERSION, ValueString);

   *response = r;

   return 0;

error:

   pgmoneta_json_destroy(r);

   return 1;
}

static int
create_outcome_success(struct json* json, time_t start_time, time_t end_time, struct json** outcome)
{
   int32_t total_seconds = 0;
   char* elapsed = NULL;
   struct json* r = NULL;

   *outcome = NULL;

   if (pgmoneta_json_create(&r))
   {
      goto error;
   }

   elapsed = pgmoneta_get_timestamp_string(start_time, end_time, &total_seconds);

   pgmoneta_json_put(r, CLUSTERING_ARGUMENT_STATUS, (uintptr_t)true, ValueBool);
   pgmoneta_json_put(r, CLUSTERING_ARGUMENT_TIME, (uintptr_t)elapsed, ValueString);

   pgmoneta_json_put(json, CLUSTERING_CATEGORY_OUTCOME, (uintptr_t)r, ValueJSON);

   *outcome = r;

   free(elapsed);

   return 0;

error:

   free(elapsed);

   pgmoneta_json_destroy(r);

   return 1;
}

static int
create_outcome_failure(struct json* json, int32_t error, struct json** outcome)
{
   struct json* r = NULL;

   *outcome = NULL;

   if (json == NULL)
   {
      if (pgmoneta_json_create(&json))
      {
         goto error;
      }
   }

   if (pgmoneta_json_create(&r))
   {
      goto error;
   }

   pgmoneta_json_put(r, CLUSTERING_ARGUMENT_STATUS, (uintptr_t)false, ValueBool);
   pgmoneta_json_put(r, CLUSTERING_ARGUMENT_ERROR, (uintptr_t)error, ValueInt32);

   pgmoneta_json_put(json, CLUSTERING_CATEGORY_OUTCOME, (uintptr_t)r, ValueJSON);

   *outcome = r;

   return 0;

error:

   pgmoneta_json_destroy(r);

   return 1;
}

static bool
is_defined_unique_id(char* id)
{
   struct configuration* config;

   config = (struct configuration*)shmem;

   if (!strcmp(config->clustering_id, id))
   {
      return true;
   }

   for (int i = 0; i < config->number_of_nodes; i++)
   {
      if (!strcmp(config->nodes[i].id, id))
      {
         return true;
      }
   }

   return false;
}

static bool
is_defined_server(char* server)
{
   struct configuration* config;

   config = (struct configuration*)shmem;

   for (int i = 0; i < config->number_of_nodes; i++)
   {
      for (int j = 0; i < config->nodes[i].number_of_servers; j++)
      {
         if (!strcmp(config->nodes[i].server_names[j], server))
         {
            return true;
         }
      }
   }

   return false;
}

static int
read_uint8(char* prefix, SSL* ssl, int socket, uint8_t* i)
{
   char buf1[1] = {0};

   *i = 0;

   if (read_complete(ssl, socket, &buf1[0], sizeof(buf1)))
   {
      pgmoneta_log_warn("%s: read_byte: %p %d %s", prefix, ssl, socket, strerror(errno));
#ifdef DEBUG
      pgmoneta_backtrace();
#endif
      errno = 0;
      goto error;
   }

   *i = pgmoneta_read_uint8(&buf1);

   pgmoneta_log_info("read_uint8: %d", *i);

   return 0;

error:

   pgmoneta_log_info("read_uint8 error");

   return 1;
}

static int
read_string(char* prefix, SSL* ssl, int socket, char** str)
{
   char* s = NULL;
   char buf4[4] = {0};
   uint32_t size = 0;

   *str = NULL;

   if (read_complete(ssl, socket, &buf4[0], sizeof(buf4)))
   {
      pgmoneta_log_warn("%s: read_string: %p %d %s", prefix, ssl, socket, strerror(errno));
#ifdef DEBUG
      pgmoneta_backtrace();
#endif
      errno = 0;
      goto error;
   }

   size = pgmoneta_read_uint32(&buf4);

   pgmoneta_log_info("read_string: %d", size);

   if (size > 0)
   {
      s = malloc(size + 1);

      if (s == NULL)
      {
         goto error;
      }

      memset(s, 0, size + 1);

      if (read_complete(ssl, socket, s, size))
      {
         pgmoneta_log_warn("%s: read_string: %p %d %s", prefix, ssl, socket, strerror(errno));
         errno = 0;
         goto error;
      }

      *str = s;
   }

   return 0;

error:

   pgmoneta_log_info("read_string: %d", size);

   free(s);

   return 1;
}

static int
write_uint8(char* prefix, SSL* ssl, int socket, uint8_t i)
{
   char buf1[1] = {0};

   pgmoneta_write_uint8(&buf1, i);

   pgmoneta_log_info("write_uint8: %d", i);

   if (write_complete(ssl, socket, &buf1, sizeof(buf1)))
   {
      pgmoneta_log_warn("%s: write_string: %p %d %s", prefix, ssl, socket, strerror(errno));
#ifdef DEBUG
      pgmoneta_backtrace();
#endif
      errno = 0;
      goto error;
   }

   return 0;

error:

   pgmoneta_log_info("write_uint8: error");

   return 1;
}

static int
write_string(char* prefix, SSL* ssl, int socket, char* str)
{
   char buf4[4] = {0};

   pgmoneta_write_uint32(&buf4, str != NULL ? strlen(str) : 0);

   pgmoneta_log_info("write_string: %d", str != NULL ? strlen(str) : 0);

   if (write_complete(ssl, socket, &buf4, sizeof(buf4)))
   {
      pgmoneta_log_warn("%s: write_string: %p %d %s", prefix, ssl, socket, strerror(errno));
#ifdef DEBUG
      pgmoneta_backtrace();
#endif
      errno = 0;
      goto error;
   }

   if (str != NULL)
   {
      if (write_complete(ssl, socket, str, strlen(str)))
      {
         pgmoneta_log_warn("%s: write_string: %p %d %s", prefix, ssl, socket, strerror(errno));
#ifdef DEBUG
         pgmoneta_backtrace();
#endif
         errno = 0;
         goto error;
      }
   }

   return 0;

error:

   pgmoneta_log_info("write_string: error");

   return 1;
}

static int
read_complete(SSL* ssl, int socket, void* buf, size_t size)
{
   ssize_t r;
   size_t offset;
   size_t needs;
   int retries;

   offset = 0;
   needs = size;
   retries = 0;

read:
   if (ssl == NULL)
   {
      r = read(socket, buf + offset, needs);
   }
   else
   {
      r = SSL_read(ssl, buf + offset, needs);
   }

   if (r == -1)
   {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
      {
         errno = 0;
         goto read;
      }

      goto error;
   }
   else if (r < (ssize_t)needs)
   {
      /* Sleep for 10ms */
      SLEEP(10000000L);

      if (retries < 100)
      {
         offset += r;
         needs -= r;
         retries++;
         goto read;
      }
      else
      {
         errno = EINVAL;
         goto error;
      }
   }

   return 0;

error:

   return 1;
}

static int
write_complete(SSL* ssl, int socket, void* buf, size_t size)
{
   if (ssl == NULL)
   {
      return write_socket(socket, buf, size);
   }

   return write_ssl(ssl, buf, size);
}

static int
write_socket(int socket, void* buf, size_t size)
{
   bool keep_write = false;
   ssize_t numbytes;
   int offset;
   ssize_t totalbytes;
   ssize_t remaining;

   numbytes = 0;
   offset = 0;
   totalbytes = 0;
   remaining = size;

   do
   {
      numbytes = write(socket, buf + offset, remaining);

      if (likely(numbytes == (ssize_t)size))
      {
         return 0;
      }
      else if (numbytes != -1)
      {
         offset += numbytes;
         totalbytes += numbytes;
         remaining -= numbytes;

         if (totalbytes == (ssize_t)size)
         {
            return 0;
         }

         pgmoneta_log_trace("Write %d - %zd/%zd vs %zd", socket, numbytes, totalbytes, size);
         keep_write = true;
         errno = 0;
      }
      else
      {
         switch (errno)
         {
            case EAGAIN:
               keep_write = true;
               errno = 0;
               break;
            default:
               keep_write = false;
               break;
         }
      }
   }
   while (keep_write);

   return 1;
}

static int
write_ssl(SSL* ssl, void* buf, size_t size)
{
   bool keep_write = false;
   ssize_t numbytes;
   int offset;
   ssize_t totalbytes;
   ssize_t remaining;

   numbytes = 0;
   offset = 0;
   totalbytes = 0;
   remaining = size;

   do
   {
      numbytes = SSL_write(ssl, buf + offset, remaining);

      if (likely(numbytes == (ssize_t)size))
      {
         return 0;
      }
      else if (numbytes > 0)
      {
         offset += numbytes;
         totalbytes += numbytes;
         remaining -= numbytes;

         if (totalbytes == (ssize_t)size)
         {
            return 0;
         }

         pgmoneta_log_trace("SSL/Write %d - %zd/%zd vs %zd", SSL_get_fd(ssl), numbytes, totalbytes, size);
         keep_write = true;
         errno = 0;
      }
      else
      {
         int err = SSL_get_error(ssl, numbytes);

         switch (err)
         {
            case SSL_ERROR_ZERO_RETURN:
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
            case SSL_ERROR_WANT_CONNECT:
            case SSL_ERROR_WANT_ACCEPT:
            case SSL_ERROR_WANT_X509_LOOKUP:
#ifndef HAVE_OPENBSD
#if (OPENSSL_VERSION_NUMBER >= 0x10100000L)
            case SSL_ERROR_WANT_ASYNC:
            case SSL_ERROR_WANT_ASYNC_JOB:
#if (OPENSSL_VERSION_NUMBER >= 0x10101000L)
            case SSL_ERROR_WANT_CLIENT_HELLO_CB:
#endif
#endif
#endif
               errno = 0;
               keep_write = true;
               break;
            case SSL_ERROR_SYSCALL:
               pgmoneta_log_error("SSL_ERROR_SYSCALL: %s (%d)", strerror(errno), SSL_get_fd(ssl));
               errno = 0;
               keep_write = false;
               break;
            case SSL_ERROR_SSL:
               pgmoneta_log_error("SSL_ERROR_SSL: %s (%d)", strerror(errno), SSL_get_fd(ssl));
               errno = 0;
               keep_write = false;
               break;
         }
         ERR_clear_error();

         if (!keep_write)
         {
            return 1;
         }
      }
   }
   while (keep_write);

   return 1;
}
