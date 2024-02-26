/*
* librdkafka - The Apache Kafka C/C++ library
*
* Copyright (c) 2015 Magnus Edenhill
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*
* 1. Redistributions of source code must retain the above copyright notice,
*    this list of conditions and the following disclaimer.
* 2. Redistributions in binary form must reproduce the above copyright notice,
*    this list of conditions and the following disclaimer in the documentation
*    and/or other materials provided with the distribution.
*
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
* AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
* IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
* ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
* LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
* CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
* SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
* INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
* CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
* ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
* POSSIBILITY OF SUCH DAMAGE.
*/

#include "rdkafka_int.h"
#include "rdkafka_assignor.h"
#include "rdrand.h"

/**
* Source: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/RoundRobinAssignor.java
*
* The roundrobin assignor lays out all the available partitions and all the
* available consumers. It then proceeds to do a roundrobin assignment from
* partition to consumer. If the subscriptions of all consumer instances are
* identical, then the partitions will be uniformly distributed. (i.e., the
* partition ownership counts will be within a delta of exactly one across all
* consumers.)
*
* For example, suppose there are two consumers C0 and C1, two topics t0 and
* t1, and each topic has 3 partitions, resulting in partitions t0p0, t0p1,
* t0p2, t1p0, t1p1, and t1p2.
*
* The assignment will be:
* C0: [t0p0, t0p2, t1p1]
* C1: [t0p1, t1p0, t1p2]
*/

rd_kafka_resp_err_t
rd_kafka_doubleroundrobin_assignor_assign_cb (rd_kafka_t *rk,
                                            const rd_kafka_assignor_t *rkas,
                                            const char *member_id,
                                            const rd_kafka_metadata_t *metadata,
                                            rd_kafka_group_member_t *members,
                                            size_t member_cnt,
                                            rd_kafka_assignor_topic_t
                                                **eligible_topics,
                                            size_t eligible_topic_cnt,
                                            char *errstr, size_t errstr_size,
                                            void *opaque) {
    unsigned int ti;

    /* The range assignor works on a per-topic basis. */
    for (ti = 0 ; ti < eligible_topic_cnt ; ti++)
    {
        rd_kafka_assignor_topic_t * eligible_topic = eligible_topics[ti];

        /* For each topic, we lay out the available partitions in
            * numeric order and the consumers in lexicographic order. */
        qsort(members, member_cnt, sizeof(*members),
              rd_kafka_group_member_cmp);

        int member_cnt = rd_list_cnt(&eligible_topic->members);

        /// save the begin and end index for each group,  for group 0, begin: group_borders[0], end: group_borders[1].
        /// [begin, end).
        int group_borders[member_cnt + 1];
        int group_count = 0;

        /// Save members after deleted replica members.
        int deleted_member_lists[member_cnt + 1];
        int deleted_member_lists_idx = 0;

        /// record the first consumer and group.
        deleted_member_lists[deleted_member_lists_idx++] = 0;
        group_borders[group_count++] = 0;

        for (int i = 1; i < member_cnt ; ++i)
        {
            int status = rd_kafka_str_member_is_replicate(members[i].rkgm_member_id->str, members[i-1].rkgm_member_id->str);
            switch (status)
            {
                case 0:
                    group_borders[group_count++] = deleted_member_lists_idx;
                    deleted_member_lists[deleted_member_lists_idx++] = i;
                    break;
                case 1:
                    deleted_member_lists[deleted_member_lists_idx++] = i;
                    break;
                case 2:
                    break;
            }
        }
        group_borders[group_count] = deleted_member_lists_idx;

        /// This array saves every consumer's size, for example:
        /// non_duplicate_members_pos = {-1, 3, 6, 8}
        /// group0 = {0, 2, 4, 6} --> size = 4
        /// group1 = {8, 10, 12} --> size = 3
        /// group2 = {14, 16} --> size = 2
        int sizeof_member_group[group_count];
        for (int group_idx = 0; group_idx < group_count; ++group_idx)
        {
            sizeof_member_group[group_idx] = group_borders[group_idx + 1] - group_borders[group_idx];
        }

        /// This array saves the relative position in each group
        /// ex: a group1 with size 5, next_in_member_group[1]: 0, 1, 2, 3, 4
        //// similar to 'next' variable for group assignment.
        int next_in_member_group[group_count];
        memset(next_in_member_group, -1, sizeof(next_in_member_group));
        int next = -1;
        int num_unassigned_partition = eligible_topic->metadata->partition_cnt;
        int num_residual_partition = eligible_topic->metadata->partition_cnt % group_count;
        for (int partition = 0; partition < eligible_topic->metadata->partition_cnt; partition++)
        {
            /// assigned to group
            if (num_unassigned_partition <= num_residual_partition) {
                next = rd_jitter(0, group_count - 1);
            }
            else {
                next = (next + 1) % group_count;
            }
            /// assigned to a group member
            next_in_member_group[next] = (next_in_member_group[next] + 1) % sizeof_member_group[next];

            /// get real member index in "members" array.
            rd_kafka_group_member_t *rkgm = &members[deleted_member_lists[group_borders[next] + next_in_member_group[next]]];

            rd_kafka_dbg(rk, CGRP, "ASSIGN",
                         "doubleroundrobin: Member \"%s\": "
                         "assigned topic %s partition %d",
                         rkgm->rkgm_member_id->str,
                         eligible_topic->metadata->topic,
                         partition);

            rd_kafka_topic_partition_list_add(
                rkgm->rkgm_assignment,
                eligible_topic->metadata->topic, partition);
            
            num_unassigned_partition--;
        }
    }
    return 0;
}



/**
 * @brief Initialzie and add doubleroundrobin assignor.
 */
rd_kafka_resp_err_t rd_kafka_doubleroundrobin_assignor_init (rd_kafka_t *rk) {
        return rd_kafka_assignor_add(
                rk, "consumer", "doubleroundrobin",
                RD_KAFKA_REBALANCE_PROTOCOL_EAGER,
                rd_kafka_doubleroundrobin_assignor_assign_cb,
                rd_kafka_assignor_get_metadata_with_empty_userdata,
                NULL, NULL, NULL, NULL);
}
