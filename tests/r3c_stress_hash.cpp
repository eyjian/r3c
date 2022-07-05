//
// Created by leolaxie on 2022/7/4.
// 测试redis hash在不同数量的field下的表现


#include "r3c.h"
#include "stop_watch.h"
#include <stdlib.h>
#include <stdio.h>
#include <vector>

#define MAX(a, b) (a) > (b) ? (a) : (b)
#define MIN(a, b) (a) < (b) ? (a) : (b)

typedef void (*CB)(r3c::CRedisClient &redis);

static void fill_cb(CB cbs[]);  // todo: multi test functionality

// 统计
typedef struct redisStat
{
    const std::string name;
    const int num_fields;
    const int num_cycles;
    uint64_t min_us;
    uint64_t max_us;
    uint64_t sum_us;

    redisStat(const std::string &name, const int numFields, const int numCycles) : name(name), num_fields(numFields),
                                                                                   num_cycles(numCycles), min_us(0),
                                                                                   max_us(0), sum_us(0) {}

    friend std::ostream &operator<<(FILE *os, const redisStat &stat)
    {
        fprintf(os, "test name: %s\n", stat.name.c_str());
        fprintf(os, "number of fields: %u\n", stat.num_fields);
        fprintf(os, "total calls: %u\n", stat.num_cycles);
        fprintf(os, "avg time cost: %lu us\n", stat.sum_us / stat.num_cycles);
        fprintf(os, "max time cost: %lu us\n", stat.max_us);
        fprintf(os, "min time cost: %lu us\n", stat.min_us);
    }
} redisStat;


// argv[1] redis nodes
// argv[2] number of fields
// argv[3] number of cycles
int main(int argc, char *argv[])
{
    if (argc != 4)
    {
        fprintf(stderr, "Usage: %s <redis nodes> <number of fields> <number of cycles>\n", argv[0]);
        fprintf(stderr, "Example: %s 127.0.0.1:6379,127.0.0.1:6380 10 100000\n", argv[0]);
        exit(1);
    } else
    {
        const int num_retries = r3c::NUM_RETRIES;
        const uint32_t expired_seconds = 3600 * 24;
        const std::string &redis_nodes = argv[1];
        const std::string the_key = "stress_hget";
        redisStat stat(the_key, atoi(argv[2]), atoi(argv[3]));
        r3c::CRedisClient the_redis(redis_nodes);
        CStopWatch the_timer;
        std::vector<std::string> fields(stat.num_fields);
        volatile uint64_t span;

        try
        {
            the_redis.hsetnx(the_key, "", ""); // init
            for (std::vector<std::string>::size_type j = 0; j < fields.size(); ++j)
            {
                char field_buf[32]; // fixed field size
                snprintf(field_buf, sizeof(field_buf), "FIELD_%zd", j);
                fields[j] = field_buf;
                the_redis.hset(the_key, fields[j], fields[j]);
            }
            the_timer.restart(); // init
            for (int j = 0; j < stat.num_cycles; ++j)
            {
                std::string val; // dumped
                the_redis.hget(the_key, fields[j % stat.num_fields], &val);
                span = the_timer.get_elapsed_microseconds();
                stat.min_us = MIN(stat.min_us, span);
                stat.max_us = MAX(stat.max_us, span);
                stat.sum_us += span;
            }
        }
        catch (r3c::CRedisException &ex)
        {
            fprintf(stderr, "%s\n", ex.str().c_str());
            exit(1);
        }
        stdout << stat;
    }
}



