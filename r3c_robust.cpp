// Writed by yijian (eyjian@qq.com)
#include "r3c.h"
#include "utils.h"
#include <libgen.h> // basename
#include <stdlib.h> // atoi
#include <string.h> // strdup

typedef void (*F)(const char* redis_nodes);
static void fill_f(F f[]);

int main(int argc, char* argv[])
{
    F f[2019];
    char* s = strdup(argv[0]);
    char* b = basename(s);

    for (size_t i=0; i<sizeof(f)/sizeof(f[0]); ++i)
        f[i] = NULL;
    fill_f(f);
    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s index redis_nodes, example: %s 1 127.0.0.1:6379\n", b, b);
        free(s);
        exit(1);
    }
    else
    {
        const int n = atoi(argv[1]);
        const char* redis_nodes = argv[2];

        if (n<0 || n>=static_cast<int>(sizeof(f)/sizeof(f[0])))
        {
            fprintf(stderr, "Usage: %s index redis_nodes, example: %s 1 127.0.0.1:6379\n", b, b);
            free(s);
            exit(1);
        }
        else
        {
            free(s);
            if (f[n] != NULL)
                (*(f[n]))(redis_nodes);
            return 0;
        }
    }
}

// 可用来观察：
// 1) 运行中，master异常
// 2) 运行中，replica异常
// 3) 运行中，master和replica都异常
void f0(const char* redis_nodes)
{
    for (int i=0; i<600; ++i)
    {
        try
        {
            const std::string k = "K1";
            r3c::CRedisClient redis(redis_nodes);
            std::string v;
            r3c::Node which;
            redis.get(k, &v, &which);
            fprintf(stdout, "[%s][slot://%d][key://%s][redis://%s] %s => %s\n",
                    r3c::get_formatted_current_datetime(true).c_str(), r3c::get_key_slot(&k),
                    k.c_str(), r3c::node2string(which).c_str(), k.c_str(), v.c_str());
        }
        catch (r3c::CRedisException& ex)
        {
            fprintf(stderr, "[%s] `get` failed: %s\n",
                    r3c::get_formatted_current_datetime(true).c_str(), ex.str().c_str());
        }

        r3c::millisleep(1000);
    }
}

void f1(const char* redis_nodes)
{
    for (int i=0; i<60; ++i)
    {
        try
        {
            r3c::CRedisClient redis(redis_nodes);
        }
        catch (r3c::CRedisException& ex)
        {
            fprintf(stderr, "%s\n", ex.str().c_str());
        }
    }
}

void fill_f(F f[])
{
    f[0] = f0;
    f[1] = f1;
}
