//
// Created by leolaxie on 2022/7/4.
// 测试redis hash在不同数量的field下的表现


#include "r3c.h"
#include "stop_watch.h"
#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <queue>

#define MAX(a, b) (a) > (b) ? (a) : (b)
#define MIN(a, b) (a) < (b) ? (a) : (b)
static const int num_top = 10; // 统计top10的查询时间

// Top
class Topk
{
public:
    Topk(int k) : _k(k) {}

    void insert(uint64_t x)
    {
        if (_q.size() < (uint64_t) _k)
        { _q.push(x); }
        else if (x > _q.top())
        {
            _q.pop();
            _q.push(x);
        }
    }

    std::vector<uint64_t> finalize()
    {
        std::vector<uint64_t> result(_q.size());
        while (_q.size())
        {
            result[_q.size() - 1] = _q.top();
            _q.pop();
        }
        return result;
    }

private:
    int _k;
    std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>> _q;
};

// 统计
typedef struct redisStat
{
    const std::string name;
    const int num_fields;
    const int num_cycles;
    uint64_t min_us;
    uint64_t max_us;
    uint64_t sum_us;
    Topk top;

    redisStat(const std::string &name, const int numFields, const int numCycles) : name(name), num_fields(numFields),
                                                                                   num_cycles(numCycles),
                                                                                   min_us(999999L),
                                                                                   max_us(0), sum_us(0), top(num_top) {}

    friend std::ostream &operator<<(FILE *os, redisStat &stat)
    {
        fprintf(os, "test name: %s\n", stat.name.c_str());
        fprintf(os, "number of fields: %u\n", stat.num_fields);
        fprintf(os, "total calls: %u\n", stat.num_cycles);
        fprintf(os, "avg time cost: %lu us\n", stat.sum_us / stat.num_cycles);
        fprintf(os, "max time cost: %lu us\n", stat.max_us);
        fprintf(os, "min time cost: %lu us\n", stat.min_us);
        fprintf(os, "top%d:\n", num_top);
        auto res = stat.top.finalize();
        for (int i = 0; i < num_top; ++i)
        {
            fprintf(os, "%lu\t", res[i]);
        }
        fprintf(os, "\n\n");
    }
} redisStat;

/// test hget
// HGET all fields
void test_hget(r3c::CRedisClient &redis_, redisStat &stat_)
{
    CStopWatch the_timer;
    std::vector<std::string> fields(stat_.num_fields);
    volatile uint64_t span;

    try
    {
        redis_.hsetnx(stat_.name, "", ""); // init
        for (std::vector<std::string>::size_type j = 0; j < fields.size(); ++j)
        {
            char field_buf[32]; // fixed field size
            snprintf(field_buf, sizeof(field_buf), "FIELD_%zd", j);
            fields[j] = field_buf;
            redis_.hset(stat_.name, fields[j], fields[j]);
        }
        the_timer.restart(); // init
        for (int j = 0; j < stat_.num_cycles; ++j)
        {
            std::map<std::string, std::string> dumped;
            redis_.hget(stat_.name, fields, &dumped);
            span = the_timer.get_elapsed_microseconds();
            stat_.min_us = MIN(stat_.min_us, span);
            stat_.max_us = MAX(stat_.max_us, span);
            stat_.sum_us += span;
            stat_.top.insert(span);
        }
    }
    catch (r3c::CRedisException &ex)
    {
        fprintf(stderr, "%s\n", ex.str().c_str());
        exit(1);
    }
    stdout << stat_;
}


/// test hgetall
// HGETALL all fields
void test_hgetall(r3c::CRedisClient &redis_, redisStat &stat_)
{
    CStopWatch the_timer;
    std::vector<std::string> fields(stat_.num_fields);
    volatile uint64_t span;

    try
    {
        redis_.hsetnx(stat_.name, "", ""); // init
        for (std::vector<std::string>::size_type j = 0; j < fields.size(); ++j)
        {
            char field_buf[32]; // fixed field size
            snprintf(field_buf, sizeof(field_buf), "FIELD_%zd", j);
            fields[j] = field_buf;
            redis_.hset(stat_.name, fields[j], fields[j]);
        }
        the_timer.restart(); // init
        for (int j = 0; j < stat_.num_cycles; ++j)
        {
            std::map<std::string, std::string> dumped;
            redis_.hgetall(stat_.name, &dumped);
            span = the_timer.get_elapsed_microseconds();
            stat_.min_us = MIN(stat_.min_us, span);
            stat_.max_us = MAX(stat_.max_us, span);
            stat_.sum_us += span;
            stat_.top.insert(span);
        }
    }
    catch (r3c::CRedisException &ex)
    {
        fprintf(stderr, "%s\n", ex.str().c_str());
        exit(1);
    }
    stdout << stat_;
}

// argv[1] redis nodes
// argc[2] redis password
// argv[3] number of fields
// argv[4] number of cycles
int main(int argc, char *argv[])
{
    if (argc != 5)
    {
        fprintf(stderr, "Usage: %s <redis nodes> <password> <number of fields> <number of cycles>\n", argv[0]);
        fprintf(stderr, "Example: %s 127.0.0.1:6379,127.0.0.1:6380 123456 10 100000\n", argv[0]);
        exit(1);
    } else
    {
        const int num_retries = r3c::NUM_RETRIES;
        const uint32_t expired_seconds = 3600 * 24;
        const std::string &redis_nodes = argv[1];
        const std::string &redis_pw = argv[2];
        r3c::CRedisClient the_redis(redis_nodes, redis_pw);
        // test which
        redisStat redisStat1("test_hget", atoi(argv[3]), atoi(argv[4]));
        test_hget(the_redis, redisStat1);
        redisStat redisStat2("test_hgetall", atoi(argv[3]), atoi(argv[4]));
        test_hgetall(the_redis, redisStat2);
    }
}
