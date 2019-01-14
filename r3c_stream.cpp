// Writed by yijian (eyjian@qq.com)
#include "r3c.h"
#include "utils.h"

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

static void testcase1(r3c::CRedisClient& redis)
{
    const std::string group = "group";
    const std::string consumer = "consumer";
    std::vector<std::string> topics(2);
    std::vector<std::string> ids(2);
    std::vector<std::pair<std::string, std::string> > values(2);
    int count = 10;

    topics[0] = "topic0";
    topics[1] = "topic1";

    // XADD
    values[0].first = "field0";
    values[0].second = "value0";
    values[1].first = "field1";
    values[1].second = "value1";
    ids[0] = redis.xadd(topics[0], "*", values);
    ids[1] = redis.xadd(topics[1], "*", values);

    // XGROUP CREATE
    try
    {
        redis.xgroup_create(topics[0], group, "$");
    }
    catch (r3c::CRedisException& ex)
    {
        if (!r3c::is_busygroup_error(ex.errtype()))
            throw;
    }
    try
    {
        redis.xgroup_create(topics[1], group, "$");
    }
    catch (r3c::CRedisException& ex)
    {
        if (!r3c::is_busygroup_error(ex.errtype()))
            throw;
    }

    // XREADGROUP
    redis.xreadgroup(group, consumer, topics, ids, count);
}

void init_testcase(TESTCASE testcase[])
{
    int i = 0;
    testcase[i++] = testcase1;
    //testcase[i++] = testcase2;
    //testcase[i++] = testcase3;
}