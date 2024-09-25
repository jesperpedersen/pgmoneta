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
#include <logging.h>

#define DATA_SIZE 8192

int
pgmoneta_bitset_create(size_t input_size, struct bitset** bitset)
{
   struct bitset* bs = NULL;
   size_t size = 0;

   bs = (struct bitset*)malloc(sizeof(struct bitset));

   if (bs == NULL)
   {
      goto error;
   }

   if (input_size % DATA_SIZE == 0)
   {
      size = input_size / DATA_SIZE;
   }
   else
   {
      size = input_size / DATA_SIZE;
      size += input_size % DATA_SIZE;
   }

   bs->input_size = input_size;
   bs->size = size;
   bs->data = (char*)malloc(bs->size);

   if (bs->data == NULL)
   {
      goto error;
   }

   memset(bs->data, 0, bs->size);
   
   *bitset = bs;

   return 0;

error:

   free(bs);

   return 1;
}

int
pgmoneta_bitset_set(struct bitset* bitset, uint64_t index)
{
   uint64_t offset;
   short bit;

   if (bitset == NULL)
   {
      goto error;
   }

   offset = (uint64_t)(index / CHAR_BIT);
   bit = 1 << (index % CHAR_BIT);

   *(bitset->data + offset) |= bit;

   return 0;

error:

   return 1;
}

bool
pgmoneta_bitset_get(struct bitset* bitset, uint64_t index)
{
   uint64_t offset;
   char bit;
   char c;

   if (bitset == NULL)
   {
      goto error;
   }

   offset = (uint64_t)(index / CHAR_BIT);
   bit = 1 << (index % CHAR_BIT);

   c = *(bitset->data + offset);
   c = c & bit;

   if (*(bitset->data + offset) < 0 && bit == -128)
   {
      return true;
   }

   return (bool)(c > 0);

error:

   return false;
}

int
pgmoneta_bitset_clear(struct bitset* bitset, uint64_t index)
{
   uint64_t offset;
   short bit;

   if (bitset == NULL)
   {
      goto error;
   }

   offset = (uint64_t)(index / CHAR_BIT);
   bit = 1 << (index % CHAR_BIT);

   *(bitset->data + offset) &= ~bit;

   return 0;

error:

   return 1;
}

void
pgmoneta_bitset_destroy(struct bitset* bs)
{
   if (bs != NULL)
   {
      free(bs->data);
      free(bs);
   }
}
