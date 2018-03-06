#include <libco/co_routine.h> // a coroutine library powered by tencent
#include <stdio.h>
#include <stdlib.h>
#include "r3c.h"

#define NUM_CYCLES 3

static int sg_num_coroutines;
static std::string sg_redis_nodes;

static void* redis_routine(void* param)
{
    co_enable_hook_sys();

    try
    {
        const int i = *(int*)param;
        const int connect_timeout_milliseconds = 20000;
        const int data_timeout_milliseconds = 20000;
        r3c::CRedisClient redis(sg_redis_nodes);

        for (int j=0; j<NUM_CYCLES; ++j)
        {
            const std::string& key = r3c::format_string("K_%d_%d", i, j);
            const std::string& value = r3c::format_string("V_%d_%d", i, j);
            redis.setex(key, value, 60);
            fprintf(stdout, "KEY[%s] => %s\n", key.c_str(), value.c_str());
        }
    }
    catch (r3c::CRedisException& ex)
    {
        fprintf(stderr, "Subcoroutine: %s\n", ex.str().c_str());
    }

    --sg_num_coroutines;
    return NULL;
}

static int break_co_eventloop(void*)
{
    return (0 == sg_num_coroutines)? -1: 0;
}

int main(int argc, char* argv[])
{
    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s num_coroutines redis_nodes\n", argv[0]);
        fprintf(stderr, "Example: %s 3 127.0.0.1:6379\n", argv[0]);
        exit(1);
    }
    else
    {
        const int num_coroutines = atoi(argv[1]);
        sg_num_coroutines = num_coroutines;
        sg_redis_nodes = argv[2];

        for (int i=0; i<num_coroutines; ++i)
        {
            stCoRoutine_t *co = 0;
            co_create(&co, NULL, redis_routine, &i);
            co_resume(co);
        }
        co_eventloop(co_get_epoll_ct(), break_co_eventloop, 0);

        fprintf(stdout, "\n");
        r3c::CRedisClient redis(sg_redis_nodes);
        for (int i=0; i<num_coroutines; ++i)
        {
            try
            {
                for (int j=0; j<NUM_CYCLES; ++j)
                {
                    const std::string& key = r3c::format_string("K_%d_%d", i, j);
                    std::string value;
                    redis.get(key, &value);
                    fprintf(stdout, "KEY[%s]: %s\n", key.c_str(), value.c_str());
                }
            }
            catch (r3c::CRedisException& ex)
            {
                fprintf(stderr, "%s\n", ex.str().c_str());
            }
        }

        return 0;
    }
}
