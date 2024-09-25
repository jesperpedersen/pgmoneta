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

#ifndef PGMONETA_BITSET_H
#define PGMONETA_BITSET_H

#ifdef __cplusplus
extern "C" {
#endif

/* pgmoneta */
#include <pgmoneta.h>

/* system */
#include <limits.h>

/** @struct bitset
 * Defines a bitset
 */
struct bitset
{
   size_t input_size; /**< The input size */
   size_t size;       /**< The size of the bitset */
   char* data;        /**< The data of the bitset */
} __attribute__ ((aligned (64)));

/**
 * Create a bitset
 * @param input_size The input size of the bitset
 * @param bitset The resulting bitset
 * @return 0 upon success, otherwise 1
 */
int
pgmoneta_bitset_create(size_t input_size, struct bitset** bs);

/**
 * Set a bit
 * @param bitset The bitset
 * @param index The index of the bit
 * @return 0 upon success, otherwise 1
 */
int
pgmoneta_bitset_set(struct bitset* bitset, uint64_t index);

/**
 * Get a bit
 * @param bitset The bitset
 * @param index The index of the bit
 * @return True if set, otherwise false
 */
bool
pgmoneta_bitset_get(struct bitset* bitset, uint64_t index);

/**
 * Clear a bit
 * @param bitset The bitset
 * @param index The index of the bit
 * @return 0 upon success, otherwise 1
 */
int
pgmoneta_bitset_clear(struct bitset* bitset, uint64_t index);

/**
 * Destroy a bitset
 * @param bitset The bitset
 */
void
pgmoneta_bitset_destroy(struct bitset* bs);

#ifdef __cplusplus
}
#endif

#endif
