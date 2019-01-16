// Writed by yijian (eyjian@qq.com)
#include "r3c.h"
#include "utils.h"
#include <iostream>

typedef void (*TESTCASE)(r3c::CRedisClient&);
static void init_testcase(TESTCASE testcase[]);
static void usage(char* argv[]);

// argv[1] redis nodes
// argv[2] specify the test case
int main(int argc, char* argv[])
{
    if (argc != 3)
    {
        usage(argv);
        exit(1);
    }
    else
    {
        try
        {
            const int n = atoi(argv[2]);
            const int num_testcases = 10;
            TESTCASE testcase[num_testcases];
            r3c::CRedisClient redis(argv[1], r3c::RP_READ_REPLICA);
            init_testcase(testcase);

            if (n<0 || n>=num_testcases-1)
            {
                usage(argv);
                exit(1);
            }
            else
            {
                (*testcase[n])(redis);
                return 0;
            }
        }
        catch (r3c::CRedisException& ex)
        {
            fprintf(stderr, "%s\n", ex.str().c_str());
            exit(1);
        }
    }
}

void usage(char* argv[])
{
    fprintf(stderr, "Usage: %s <redis nodes> testcase\n", argv[0]);
    fprintf(stderr, "Example: %s 192.168.1.61:6379,192.168.1.62:6379 1\n", argv[0]);
}


// test xreadgroup
static void testcase0(r3c::CRedisClient& redis)
{
    const std::string group = "group";
    const std::string consumer = "consumer";
    std::vector<std::string> keys(2);
    std::vector<std::string> ids(2);
    std::vector<r3c::FVPair> fvpairs(3);
    int count = 10;

    keys[0] = "k0";
    keys[1] = "k1";

    // XADD
    fvpairs[0].field = "field00";
    fvpairs[0].value = "value00";
    fvpairs[1].field = "field01";
    fvpairs[1].value = "value01";
    fvpairs[2].field = "field02";
    fvpairs[2].value = "value02";
    ids[0] = redis.xadd(keys[0], "*", fvpairs);

    fvpairs[0].field = "field10";
    fvpairs[0].value = "value10";
    fvpairs[1].field = "field11";
    fvpairs[1].value = "value11";
    fvpairs[2].field = "field12";
    fvpairs[2].value = "value12";
    ids[1] = redis.xadd(keys[1], "*", fvpairs);

    // XGROUP CREATE
    try
    {
        redis.xgroup_create(keys[0], group, "$");
    }
    catch (r3c::CRedisException& ex)
    {
        if (!r3c::is_busygroup_error(ex.errtype()))
            throw;
    }
    try
    {
        redis.xgroup_create(keys[1], group, "$");
    }
    catch (r3c::CRedisException& ex)
    {
        if (!r3c::is_busygroup_error(ex.errtype()))
            throw;
    }

    // XREADGROUP
    ids[0] = ">";
    ids[1] = ">";
    std::vector<r3c::Stream> values;
    redis.xreadgroup(group, consumer, keys, ids, count, &values);
    std::cout << values << std::endl;
}

// test xreadgroup
static void testcase1(r3c::CRedisClient& redis)
{
    const std::string group = "group";
    const std::string consumer = "consumer";
    std::vector<std::string> keys(1);
    std::vector<r3c::FVPair> fvpairs(3);
    int count = 10;
    int64_t block_milliseconds = 0;
    bool noack = true;

    keys[0] = "k000";

    // XGROUP CREATE
    try
    {
        redis.xgroup_create(keys[0], group, "$", true);
    }
    catch (r3c::CRedisException& ex)
    {
        if (!r3c::is_busygroup_error(ex.errtype()))
            throw;
    }

    // XADD
    fvpairs[0].field = "field00";
    fvpairs[0].value = "value00";
    fvpairs[1].field = "field01";
    fvpairs[1].value = "value01";
    fvpairs[2].field = "field02";
    fvpairs[2].value = "value02";
    redis.xadd(keys[0], "*", fvpairs);

    // XREADGROUP
    std::vector<r3c::StreamEntry> values;
    redis.xreadgroup(group, consumer, keys[0], count, block_milliseconds, noack, &values);
    std::cout << values << std::endl;
}

// test xread
static void testcase2(r3c::CRedisClient& redis)
{
    std::vector<std::string> keys(2);
    std::vector<std::string> ids(2);
    std::vector<r3c::FVPair> fvpairs(3);
    int count = 10;

    keys[0] = "k2";
    keys[1] = "k3";

    // XADD
    fvpairs[0].field = "field00";
    fvpairs[0].value = "value00";
    fvpairs[1].field = "field01";
    fvpairs[1].value = "value01";
    fvpairs[2].field = "field02";
    fvpairs[2].value = "value02";
    ids[0] = redis.xadd(keys[0], "*", fvpairs);

    fvpairs[0].field = "field10";
    fvpairs[0].value = "value10";
    fvpairs[1].field = "field11";
    fvpairs[1].value = "value11";
    fvpairs[2].field = "field12";
    fvpairs[2].value = "value12";
    ids[1] = redis.xadd(keys[1], "*", fvpairs);

    // XREAD
    ids[0] = "0-0";
    ids[1] = "0-0";
    std::vector<r3c::Stream> values;
    redis.xread(keys, ids, count, &values);
    std::cout << values << std::endl;
}

// test xrange
static void testcase3(r3c::CRedisClient& redis)
{
    std::string topic = "k4";
    std::string start, end;
    std::vector<r3c::FVPair> fvpairs(3);
    std::vector<std::string> ids(2);
    std::vector<r3c::StreamEntry> values;

    // XADD
    fvpairs[0].field = "field00";
    fvpairs[0].value = "value00";
    fvpairs[1].field = "field01";
    fvpairs[1].value = "value01";
    fvpairs[2].field = "field02";
    fvpairs[2].value = "value02";
    redis.xadd(topic, "*", fvpairs);

    // XADD
    fvpairs[0].field = "field10";
    fvpairs[0].value = "value10";
    fvpairs[1].field = "field11";
    fvpairs[1].value = "value11";
    fvpairs[2].field = "field12";
    fvpairs[2].value = "value12";
    redis.xadd(topic, "*", fvpairs);

    start = "-";
    end = "+";
    redis.xrange(topic, start, end, &values);
    std::cout << values << std::endl;
}

void init_testcase(TESTCASE testcase[])
{
    int i = 0;
    testcase[i++] = testcase0;
    testcase[i++] = testcase1;
    testcase[i++] = testcase2;
    testcase[i++] = testcase3;
}
