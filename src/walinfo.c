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

#include <pgmoneta.h>
#include <configuration.h>
#include <deque.h>
#include <logging.h>
#include <shmem.h>
#include <utils.h>
#include <walfile.h>

#include <inttypes.h>
#include <err.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define OPT_COLOR 1000

static void
version(void)
{
   printf("pgmoneta-walinfo %s\n", VERSION);
   exit(1);
}

static void
usage(void)
{
   printf("pgmoneta-walinfo %s\n", VERSION);
   printf("  Command line utility to read and display Write-Ahead Log (WAL) files\n");
   printf("\n");

   printf("Usage:\n");
   printf("  pgmoneta-walinfo <file>\n");
   printf("\n");
   printf("Options:\n");
   printf("  -c, --config CONFIG_FILE Set the path to the pgmoneta.conf file\n");
   printf("  -o, --output FILE        Output file\n");
   printf("  -F, --format             Output format (raw, json)\n");
   printf("  -L, --logfile FILE       Set the log file\n");
   printf("  -q, --quiet              No output only result\n");
   printf("      --color              Use colors (on, off)\n");
   printf("  -r, --rmgr               Filter on a resource manager\n");
   printf("  -s, --start              Filter on a start LSN\n");
   printf("  -e, --end                Filter on an end LSN\n");
   printf("  -x, --xid                Filter on an XID\n");
   printf("  -l, --limit              Limit number of outputs\n");
   printf("  -v, --verbose            Output result\n");
   printf("  -V, --version            Display version information\n");
   printf("  -?, --help               Display help\n");
   printf("\n");
   printf("pgmoneta: %s\n", PGMONETA_HOMEPAGE);
   printf("Report bugs: %s\n", PGMONETA_ISSUES);
}

int
main(int argc, char** argv)
{
   int c;
   int option_index = 0;
   int ret;
   char* configuration_path = NULL;
   char* output = NULL;
   char* format = NULL;
   char* logfile = NULL;
   bool quiet = false;
   bool color = true;
   struct deque* rms = NULL;
   uint64_t start_lsn = 0;
   uint64_t end_lsn = 0;
   uint64_t start_lsn_high = 0;
   uint64_t start_lsn_low = 0;
   uint64_t end_lsn_high = 0;
   uint64_t end_lsn_low = 0;
   struct deque* xids = NULL;
   uint32_t limit = 0;
   bool verbose = false;
   enum value_type type = ValueString;
   size_t size;
   struct configuration* config = NULL;

   if (argc < 2)
   {
      usage();
      goto error;
   }

   while (1)
   {
      static struct option long_options[] =
      {
         {"configuration", required_argument, 0, 'c'},
         {"output", required_argument, 0, 'o'},
         {"format", required_argument, 0, 'F'},
         {"logfile", required_argument, 0, 'L'},
         {"quiet", no_argument, 0, 'q'},
         {"color", required_argument, 0, OPT_COLOR},
         {"rmgr", required_argument, 0, 'r'},
         {"start", required_argument, 0, 's'},
         {"end", required_argument, 0, 'e'},
         {"xid", required_argument, 0, 'x'},
         {"limit", required_argument, 0, 'l'},
         {"verbose", no_argument, 0, 'v'},
         {"version", no_argument, 0, 'V'},
         {"help", no_argument, 0, '?'},
         {0, 0, 0, 0}
      };

      c = getopt_long(argc, argv, "c:qvV?:o:F:L:r:s:e:x:l:",
                      long_options, &option_index);

      if (c == -1)
      {
         break;
      }

      switch (c)
      {
         case 'c':
            configuration_path = optarg;
            break;
         case 'o':
            output = optarg;
            break;
         case 'F':
            format = optarg;

            if (!strcmp(format, "json"))
            {
               type = ValueJSON;
            }
            else
            {
               type = ValueString;
            }

            break;
         case 'L':
            logfile = optarg;
            break;
         case 'q':
            quiet = true;
            break;
         case OPT_COLOR:
            if (!strcmp(optarg, "off"))
            {
               color = false;
            }
            else
            {
               color = true;
            }
            break;
         case 'r':
            if (rms == NULL)
            {
               if (pgmoneta_deque_create(false, &rms))
               {
                  exit(1);
               }
            }

            pgmoneta_deque_add(rms, NULL, (uintptr_t)optarg, ValueString);

            break;
         case 's':
            if (strchr(optarg, '/'))
            {
               // Assuming optarg is a string like "16/B374D848"
               if (sscanf(optarg, "%" SCNx64 "/%" SCNx64, &start_lsn_high, &start_lsn_low) == 2)
               {
                  start_lsn = (start_lsn_high << 32) + start_lsn_low;
               }
               else
               {
                  fprintf(stderr, "Invalid start LSN format\n");
                  exit(1);
               }
            }
            else
            {
               start_lsn = strtoull(optarg, NULL, 10);    // Assuming optarg is a decimal number
            }
            break;
         case 'e':
            if (strchr(optarg, '/'))
            {
               // Assuming optarg is a string like "16/B374D848"
               if (sscanf(optarg, "%" SCNx64 "/%" SCNx64, &end_lsn_high, &end_lsn_low) == 2)
               {
                  end_lsn = (end_lsn_high << 32) + end_lsn_low;
               }
               else
               {
                  fprintf(stderr, "Invalid end LSN format\n");
                  exit(1);
               }
            }
            else
            {
               end_lsn = strtoull(optarg, NULL, 10);    // Assuming optarg is a decimal number
            }
            break;
         case 'x':
            if (xids == NULL)
            {
               if (pgmoneta_deque_create(false, &xids))
               {
                  exit(1);
               }
            }

            pgmoneta_deque_add(xids, NULL, (uintptr_t)pgmoneta_atoi(optarg), ValueUInt32);

            break;
         case 'l':
            limit = pgmoneta_atoi(optarg);
            break;
         case 'v':
            verbose = true;
            break;
         case 'V':
            version();
            exit(0);
         case '?':
            usage();
            exit(0);
         default:
            break;
      }
   }

   size = sizeof(struct configuration);
   if (pgmoneta_create_shared_memory(size, HUGEPAGE_OFF, &shmem))
   {
      warnx("Error creating shared memory");
      goto error;
   }
   pgmoneta_init_configuration(shmem);

   if (configuration_path != NULL)
   {
      ret = pgmoneta_read_configuration(shmem, configuration_path);
      if (ret)
      {
         warnx("Configuration not found: %s", configuration_path);
         exit(1);
      }

      if (logfile)
      {
         config = (struct configuration*)shmem;

         config->log_type = PGMONETA_LOGGING_TYPE_FILE;
         memset(&config->log_path[0], 0, MISC_LENGTH);
         memcpy(&config->log_path[0], logfile, MIN(MISC_LENGTH - 1, strlen(logfile)));
      }

      if (pgmoneta_start_logging())
      {
         exit(1);
      }

      config = (struct configuration*)shmem;
   }
   else
   {
      ret = pgmoneta_read_configuration(shmem, "/etc/pgmoneta/pgmoneta.conf");
      if (ret)
      {
         warnx("Configuration must be specified");
         exit(1);
      }
      else
      {
         configuration_path = "/etc/pgmoneta/pgmoneta.conf";

         if (logfile)
         {
            config = (struct configuration*)shmem;

            config->log_type = PGMONETA_LOGGING_TYPE_FILE;
            memset(&config->log_path[0], 0, MISC_LENGTH);
            memcpy(&config->log_path[0], logfile, MIN(MISC_LENGTH - 1, strlen(logfile)));
         }

         if (pgmoneta_start_logging())
         {
            exit(1);
         }
      }
   }

   if (optind < argc)
   {
      char* file_path = argv[optind];

      if (pgmoneta_describe_walfile(file_path, type, output, quiet, color,
                                    rms, start_lsn, end_lsn, xids, limit))
      {
         fprintf(stderr, "Error while reading/describing WAL file\n");
         goto error;
      }
   }
   else
   {
      fprintf(stderr, "Missing <file> argument\n");
      usage();
      goto error;
   }
   pgmoneta_destroy_shared_memory(shmem, size);

   if (logfile)
   {
      pgmoneta_stop_logging();
   }

   if (verbose)
   {
      printf("Success\n");
   }

   pgmoneta_deque_destroy(rms);
   pgmoneta_deque_destroy(xids);

   return 0;

error:
   if (logfile)
   {
      pgmoneta_stop_logging();
   }

   pgmoneta_deque_destroy(rms);
   pgmoneta_deque_destroy(xids);

   if (shmem != NULL)
   {
      pgmoneta_destroy_shared_memory(shmem, size);
   }

   if (verbose)
   {
      printf("Failure\n");
   }

   return 1;
}
