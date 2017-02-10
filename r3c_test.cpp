/*
 * Copyright (c) 2016, Jian Yi <eyjian at gmail dot com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
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

// Usage1: r3c_test redis_cluster_nodes, example: r3c_test 127.0.0.1:6379,127.0.0.1:6380
// Usage2: set enviroment variable REDIS_CLUSTER_NODES, example: export REDIS_CLUSTER_NODES=127.0.0.1:6379,127.0.0.1:6380,
//         and run without any parameter.
// To test slots, please set environment varialbe TEST_SLOSTS to 1.
#include "r3c.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define PRECISION 0.000001

#define TIPS_PRINT() tips_print(__FUNCTION__)
#define ERROR_PRINT(format, ...) error_print(__FILE__, __LINE__, __FUNCTION__, format, __VA_ARGS__)
#define SUCCESS_PRINT(format, ...) success_print(__FILE__, __LINE__, __FUNCTION__, format, __VA_ARGS__)

static void tips_print(const char* function);
static void error_print(const char* file, int line, const char* function, const char* format, ...);
static void success_print(const char* file, int line, const char* function, const char* format, ...);

////////////////////////////////////////////////////////////////////////////
// MISC
static void test_slots(const std::string& redis_cluster_nodes);

// EVAL
static void test_eval(const std::string& redis_cluster_nodes);

////////////////////////////////////////////////////////////////////////////
// KEY VALUE
static void test_expire(const std::string& redis_cluster_nodes);
static void test_get_and_set1(const std::string& redis_cluster_nodes);
static void test_get_and_set2(const std::string& redis_cluster_nodes);
static void test_incrby(const std::string& redis_cluster_nodes);
static void test_setnxex(const std::string& redis_cluster_nodes);

////////////////////////////////////////////////////////////////////////////
// LIST
static void test_list(const std::string& redis_cluster_nodes);

////////////////////////////////////////////////////////////////////////////
// HASH
static void test_hget_and_hset1(const std::string& redis_cluster_nodes);
static void test_hget_and_hset2(const std::string& redis_cluster_nodes);
static void test_hmget_and_hmset1(const std::string& redis_cluster_nodes);
static void test_hmget_and_hmset2(const std::string& redis_cluster_nodes);
static void test_hscan(const std::string& redis_cluster_nodes);
static void test_hincrby_and_hlen(const std::string& redis_cluster_nodes);
static void test_hmincrby(const std::string& redis_cluster_nodes);

////////////////////////////////////////////////////////////////////////////
// SET
static void test_set(const std::string& redis_cluster_nodes);

////////////////////////////////////////////////////////////////////////////
// SORTED SET
static void test_sorted_set(const std::string& redis_cluster_nodes);
static void test_zrange(const std::string& redis_cluster_nodes);
static void test_zrevrange(const std::string& redis_cluster_nodes);
static void test_zrem(const std::string& redis_cluster_nodes);

static void my_log_write(const char* format, ...)
{
    time_t seconds = time(NULL);
    struct tm* now = localtime(&seconds);
    printf("[%02d:%02d:%02d]", now->tm_hour, now->tm_min, now->tm_sec);

    va_list ap;
    va_start(ap, format);
    vprintf(format, ap);
    va_end(ap);
}

int main(int argc, char* argv[])
{
    std::string redis_cluster_nodes;

    if (2 == argc)
    {
        redis_cluster_nodes = argv[1];
    }
    else
    {
        const char* str = getenv("REDIS_CLUSTER_NODES");
        if (str != NULL)
            redis_cluster_nodes = str;
        else
            redis_cluster_nodes = "127.0.0.1:6379,127.0.0.1:6380";
    }

    r3c::set_info_log_write(my_log_write);
    r3c::set_debug_log_write(my_log_write);

    ////////////////////////////////////////////////////////////////////////////
    // EVAL
    test_eval(redis_cluster_nodes);

    ////////////////////////////////////////////////////////////////////////////
    // KEY VALUE
    test_expire(redis_cluster_nodes);
    test_get_and_set1(redis_cluster_nodes);
    test_get_and_set2(redis_cluster_nodes);
    test_incrby(redis_cluster_nodes);
    test_setnxex(redis_cluster_nodes);

    ////////////////////////////////////////////////////////////////////////////
    // LIST
    test_list(redis_cluster_nodes);

    ////////////////////////////////////////////////////////////////////////////
    // HASH
    test_hget_and_hset1(redis_cluster_nodes);
    test_hget_and_hset2(redis_cluster_nodes);
    test_hmget_and_hmset1(redis_cluster_nodes);
    test_hmget_and_hmset2(redis_cluster_nodes);
    test_hscan(redis_cluster_nodes);
    test_hincrby_and_hlen(redis_cluster_nodes);
    test_hmincrby(redis_cluster_nodes);

    ////////////////////////////////////////////////////////////////////////////
    // SET
    test_set(redis_cluster_nodes);

    ////////////////////////////////////////////////////////////////////////////
    // SORTED SET
    test_sorted_set(redis_cluster_nodes);
    test_zrange(redis_cluster_nodes);
    test_zrevrange(redis_cluster_nodes);
    test_zrem(redis_cluster_nodes);

    ////////////////////////////////////////////////////////////////////////////
    // MISC
    const char* test_slots_env = getenv("TEST_SLOSTS");
    if ((test_slots_env != NULL) && (0 == strcmp(test_slots_env, "1")))
        test_slots(redis_cluster_nodes);

    return 0;
}

struct X
{
    int a;
    short b;
    char c;
    long d;

    X()
        : a(0), b(0), c(0), d(0)
    {
    }

    X(int a_, short b_, char c_, long d_)
        : a(a_), b(b_), c(c_), d(d_)
    {
    }

    std::string str() const
    {
        return r3c::format_string("X://%d-%d-%d-%ld", a, (int)b, (int)c, d);
    }

    bool operator ==(const struct X& oth) const
    {
        return oth.a == a && oth.b == b && oth.c == c && oth.d == d;
    }

    bool operator !=(const struct X& oth) const
    {
        return oth.a != a || oth.b != b || oth.c != c || oth.d != d;
    }
};

void tips_print(const char* function)
{
    fprintf(stdout, "\n========%s========\n", function);
}

void error_print(const char* file, int line, const char* function, const char* format, ...)
{
    va_list ap;
    va_start(ap, format);

    printf(PRINT_COLOR_RED"[%s:%d][%s]", file, line, function);
    vprintf(format ,ap);
    printf(PRINT_COLOR_NONE);
    printf("\n");
    va_end(ap);
}

void success_print(const char* file, int line, const char* function, const char* format, ...)
{
    va_list ap;
    va_start(ap, format);

    printf(PRINT_COLOR_YELLOW"[%s:%d][%s]", file, line, function);
    vprintf(format ,ap);
    printf(PRINT_COLOR_NONE);
    printf("\n");
    va_end(ap);
}

////////////////////////////////////////////////////////////////////////////
// MISC
void test_slots(const std::string& redis_cluster_nodes)
{
    // test all slots attached with a node
    TIPS_PRINT();

    r3c::CRedisClient rc(redis_cluster_nodes);
    for (unsigned int i=0; i<100000000; ++i)
    {
        const std::string key = r3c::any2string(i);
        unsigned int slot = r3c::get_key_slot(&key);

        try
        {
            rc.exists(key);
        }
        catch (r3c::CRedisException& ex)
        {
            ERROR_PRINT("[%u][%u][%s]ERROR: %s", i, slot, key.c_str(), ex.str().c_str());
            return;
        }
    }

    SUCCESS_PRINT("%s", "OK");
}

////////////////////////////////////////////////////////////////////////////
// EVAL
void test_eval(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        const uint32_t timeout_seconds = 3;
        const std::string key = "eval_key";
        const std::string lua_scripts = r3c::format_string("local n; n=redis.call('incrby','%s','2016');redis.call('expire','%s','%u'); return n;", key.c_str(), key.c_str(), timeout_seconds);
        rc.del(key);
        const r3c::RedisReplyHelper redis_reply = rc.eval(key, lua_scripts);
        if (!redis_reply)
        {
            ERROR_PRINT("%s", "EVAL ERROR");
            return;
        }

        if (redis_reply->type != REDIS_REPLY_INTEGER)
        {
            ERROR_PRINT("%s", "EVAL RUN ERROR");
            return;
        }
        if (redis_reply->integer != 2016)
        {
            ERROR_PRINT("%s", "EVAL RESULT ERROR");
            return;
        }

        sleep(1);

        std::string value1;
        if (!rc.get(key, &value1))
        {
            ERROR_PRINT("%s", "EVAL GET1 ERROR");
            return;
        }
        if (value1 != "2016")
        {
            ERROR_PRINT("%s: %s", "EVAL ERROR VALUE: ", value1.c_str());
            return;
        }

        sleep(timeout_seconds);
        std::string value2;
        if (rc.get(key, &value2))
        {
            ERROR_PRINT("%s", "EVAL GET2 ERROR");
            return;
        }

        rc.del(key);
        SUCCESS_PRINT("%s", "OK");
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

////////////////////////////////////////////////////////////////////////////
// KEY VALUE
void test_expire(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        const std::string key = "r3c_kk";
        const std::string value = "123456";

        rc.del(key);
        if (rc.expire(key, 1u))
        {
            ERROR_PRINT("%s", "SET EXPIRE ERROR");
            return;
        }

        rc.set(key, value);
        if (!rc.exists(key))
        {
            ERROR_PRINT("%s", "NOT EXIST");
            return;
        }

        if (!rc.expire(key, 3u))
        {
            ERROR_PRINT("%s", "NOT EXIST");
            return;
        }

        r3c::millisleep(1000u);
        int ttl = rc.ttl(key);
        if ((ttl > 2) || (ttl < -2))
            ERROR_PRINT("%s: %d", "TTL", ttl);

        r3c::millisleep(3000u);
        if (rc.exists(key))
            ERROR_PRINT("%s", "EXIST");
        else
            SUCCESS_PRINT("%s: %d", "OK", ttl);
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

void test_get_and_set1(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        const std::string key = "r3c_kk";
        const std::string value = "123456";

        rc.del(key);
        rc.set(key, value);

        if (!rc.exists(key))
        {
            ERROR_PRINT("%s", "NOT EXIST");
            return;
        }

        std::string result;
        if (!rc.get(key, &result))
        {
            ERROR_PRINT("%s", "NOT EXIST");
        }
        else
        {
            const struct X* p = reinterpret_cast<const struct X*>(result.data());
            if (rc.del(key))
                SUCCESS_PRINT("OK: %s", p->str().c_str());
            else
                ERROR_PRINT("OK: %s", p->str().c_str());
        }
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

void test_get_and_set2(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        int k = 20160603;
        struct X v(1, 3, 5, 7);

        std::string key;
        std::string value;
        key.assign((char*)&k, sizeof(k));
        value.assign((char*)&v, sizeof(v));
        rc.del(key);
        rc.set(key, value);

        value.clear();
        if (!rc.get(key, &value))
        {
            ERROR_PRINT("%s", "not exists");
            return;
        }

        rc.del(key);
        struct X v2;
        memcpy(&v2, value.c_str(), value.size());

        if (v == v2)
            SUCCESS_PRINT("OK: %s", v2.str().c_str());
        else
            ERROR_PRINT("ERROR: %s", v2.str().c_str());
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

void test_incrby(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        const uint32_t expired_seconds = 2;
        const std::string key = "r3c kk";
        std::string value;

        rc.del(key);
        int64_t n = rc.incrby(key, 2016);
        if (n != 2016)
        {
            ERROR_PRINT("%s", "incrby ERROR1");
            return;
        }

        sleep(2);
        if (!rc.get(key, &value) && (value != "2016"))
        {
            ERROR_PRINT("%s", "incrby ERROR2");
            return;
        }

        value.clear();
        rc.del(key);
        n = rc.incrby(key, 2016, 2016, expired_seconds);
        if (n != 2016)
        {
            ERROR_PRINT("%s", "incrby ERROR3");
            return;
        }

        sleep(3);
        value.clear();
        if (rc.get(key, &value))
        {
            ERROR_PRINT("incrby ERROR4: %s", value.c_str());
            rc.del(key);
            return;
        }

        SUCCESS_PRINT("%s", "OK");
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

void test_setnxex(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        const std::string key = "r3c kk";
        std::string value;

        rc.del(key);
        if (!rc.setnxex(key, "v", 2))
        {
            ERROR_PRINT("%s", "setnxex ERROR1");
            return;
        }
        if (!rc.get(key, &value))
        {
            ERROR_PRINT("%s", "setnxex ERROR2");
            return;
        }
        if (value != "v")
        {
            ERROR_PRINT("%s", "setnxex ERROR3");
            rc.del(key);
            return;
        }

        sleep(3);
        value.clear();
        if (rc.get(key, &value))
        {
            ERROR_PRINT("setnxex ERROR4: %s", value.c_str());
            return;
        }

        SUCCESS_PRINT("%s", "OK");
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

////////////////////////////////////////////////////////////////////////////
// LIST
void test_list(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        const std::string key = "r3c kk";
        std::vector<std::string> values;
        std::string value;
        std::string str;

        rc.del(key);
        if (rc.llen(key) != 0)
        {
            ERROR_PRINT("%s", "exists");
            return;
        }

        if (rc.rpop(key, &value) || !value.empty())
        {
            ERROR_PRINT("%s", "rpop ERROR");
            return;
        }

        if (rc.lpop(key, &value) || !value.empty())
        {
            ERROR_PRINT("%s", "lpop ERROR");
            return;
        }

        if (!rc.ltrim(key, 0, 100))
        {
            ERROR_PRINT("%s", "ltrim ERROR");
            return;
        }

        if ((rc.lrange(key, 0, 10, &values) != 0) || (!values.empty()))
        {
            ERROR_PRINT("%s", "exists");
            return;
        }

        rc.lpush(key, "1");
        if ((rc.lrange(key, 0, 10, &values) != 1) || (values.size() != 1))
        {
            ERROR_PRINT("%s", "size error");
            return;
        }
        if (values[0] != "1")
        {
            ERROR_PRINT("value error: %s", values[0].c_str());
            return;
        }

        rc.lpush(key, "2");

        int m = 2016;
        str.assign((char*)&m, sizeof(m));
        rc.lpush(key, str);
        if ((rc.lrange(key, 0, 10, &values) != 3) || (values.size() != 3))
        {
            ERROR_PRINT("%s", "size error");
            return;
        }

        int* p = (int*)values[0].c_str();
        if ((*p != m) || (values[1] != "2") || (values[2] != "1"))
        {
            ERROR_PRINT("values error: %d, %s, %s", *p, values[1].c_str(), values[2].c_str());
            return;
        }

        if (!rc.rpop(key, &value))
        {
            ERROR_PRINT("%s", "rpop error");
            return;
        }
        if (value != "1")
        {
            ERROR_PRINT("value error: %s", value.c_str());
            return;
        }
        if (!rc.lpop(key, &value))
        {
            ERROR_PRINT("%s", "lpop error");
            return;
        }

        p = (int*)value.c_str();
        if (*p != m)
        {
            ERROR_PRINT("value error: %d", *p);
            return;
        }

        if (!rc.lpop(key, &value))
        {
            ERROR_PRINT("%s", "lpop error");
            return;
        }
        if (value != "2")
        {
            ERROR_PRINT("value error: %s", value.c_str());
            return;
        }

        if (rc.lpop(key, &value))
        {
            ERROR_PRINT("%s", "lpop error");
            return;
        }
        if (rc.rpop(key, &value))
        {
            ERROR_PRINT("%s", "rpop error");
            return;
        }

        rc.del(key);
        SUCCESS_PRINT("%s", "OK");
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

////////////////////////////////////////////////////////////////////////////
// HASH
void test_hget_and_hset1(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        const std::string key = "r3c_kk";
        const std::string field = "fiel d";
        const std::string value = "12345 6";
        std::string str;

        rc.del(key);
        if (rc.hget(key, field, &str))
        {
            ERROR_PRINT("%s", "EXIST");
            return;
        }

        if (rc.hexists(key, field))
        {
            ERROR_PRINT("%s", "EXIST");
            return;
        }

        if (!rc.hset(key, field, value))
        {
            ERROR_PRINT("%s", "SET ERROR");
            return;
        }

        if (!rc.hget(key, field, &str))
        {
            ERROR_PRINT("%s", "NOT EXISTS");
        }

        if (rc.hdel(key, field))
            SUCCESS_PRINT("%s", "OK");
        else
            ERROR_PRINT("%s", "DEL ERROR");

        rc.del(key);
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

void test_hget_and_hset2(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        int k = 603;
        int f = 2016;
        struct X v(2, 4, 6, 8);

        std::string key((char*)&k, sizeof(k));
        std::string field((char*)&f, sizeof(f));
        std::string value((char*)&v, sizeof(v));

        rc.del(key);
        if (!rc.hset(key, field, value))
        {
            ERROR_PRINT("%s", "hset ERROR");
            return;
        }

        value.c_str();
        if (!rc.hget(key, field, &value))
        {
            ERROR_PRINT("%s", "not exist");
            return;
        }

        struct X v2;
        memcpy(&v2, value.c_str(), value.size());

        if (v2 != v)
            ERROR_PRINT("NOT EQUAL: %s", v2.str().c_str());
        else
            SUCCESS_PRINT("OK: %s", v2.str().c_str());

        rc.del(key);
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

void test_hmget_and_hmset1(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        const std::string key = "r3c_kk";
        const std::string f[3] = { "field 1", "field 2", "field 3" };
        const std::string v[3] = { "abc 1", "abc 2", "abc 3" };
        std::vector<std::string> fields(3);
        std::map<std::string, std::string> map;
        std::map<std::string, std::string>::iterator iter;
        fields[0] = f[0];
        fields[1] = f[1];
        fields[2] = f[2];

        rc.del(key);
        int count = rc.hmget(key, fields, &map);
        printf("count: %d/%zd\n", count, map.size());
        if (!map.empty())
        {
            ERROR_PRINT("EXISTS�� %d", count);
            return;
        }

        map.clear();
        map[fields[0]] = v[0];
        map[fields[1]] = v[1];
        map[fields[2]] = v[2];
        rc.hmset(key, map);

        count = rc.hmget(key, fields, &map);
        printf("count: %d/%zd\n", count, map.size());
        if (map.empty())
        {
            ERROR_PRINT("%s", "NOT EXISTS");
            return;
        }

        if ((map[fields[0]] != v[0]) ||
            (map[fields[1]] != v[1]) ||
            (map[fields[2]] != v[2]))
        {
            ERROR_PRINT("%s", "hmget ERROR");
            return;
        }
        else
        {
            SUCCESS_PRINT("OK: %s,%s,%s", map[fields[0]].c_str(), map[fields[1]].c_str(), map[fields[2]].c_str());
        }

        map.clear();
        fields.push_back("xxx");
        count = rc.hmget(key, fields, &map, true);
        printf("count: %d/%zd\n", count, map.size());
        if ((count != 4) && (map.size() != 4))
        {
            ERROR_PRINT("%s: %d/%zd", "null field ERROR", count, map.size());
            return;
        }

        iter = map.find("xxx");
        if (iter == map.end())
        {
            ERROR_PRINT("%s", "null field not exists");
            return;
        }
        if (!iter->second.empty())
        {
            ERROR_PRINT("%s", "null field with value");
            return;
        }

        map.clear();
        count = rc.hmget(key, fields, &map, false);
        if (map.size() != 3)
        {
            ERROR_PRINT("%s", "null field ERROR");
            return;
        }
        iter = map.find("xxx");
        if (iter != map.end())
        {
            ERROR_PRINT("%s", "null field returned");
            return;
        }

        rc.del(key);
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

void test_hmget_and_hmset2(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        SUCCESS_PRINT("%s", "OK");
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

void test_hscan(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        std::map<std::string, std::string> map, results;
        std::map<std::string, std::string>::iterator iter;
        std::string key = "r3c_kk";
        int cursor = -1;

        map["name"] = "jack";
        map["age"] = "33";
        map["city"] = "shenzhen";
        map["company"] = "tencent";
        map["hobby"] = "astronomy";
        map["car"] = "bmw";
        map["nationality"] = "China";

        rc.del(key);
        rc.hmset(key, map);
        cursor = rc.hscan(key, 0, &results);
        if (cursor != 0)
        {
            ERROR_PRINT("cursor error: %d\n", cursor);
            return;
        }
        if (results.size() != map.size())
        {
            ERROR_PRINT("cursor error: %zd/%zd\n", results.size(), map.size());
            return;
        }
        if (results != map)
        {
            ERROR_PRINT("%s", "not equal\n");
            return;
        }

        cursor = rc.hscan(key, 0, "na*", &results);
        if (results.size() != 2)
        {
            ERROR_PRINT("cursor error: %zd\n", results.size());
            return;
        }
        iter = results.begin();
        if ((iter->first != "name") || (iter->second != "jack"))
        {
            ERROR_PRINT("%s", iter->first.c_str(), iter->second.c_str());
            return;
        }
        ++iter;
        if ((iter->first != "nationality") || (iter->second != "China"))
        {
            ERROR_PRINT("%s", iter->first.c_str(), iter->second.c_str());
            return;
        }

        rc.del(key);
        SUCCESS_PRINT("%s", "OK");
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

void test_hincrby_and_hlen(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        const std::string key = "r3c_kk";
        std::string str_value;
        int64_t int_value = -1;
        int count = -1;

        // hlen
        count = rc.hlen(key);
        if (count != 0)
        {
            ERROR_PRINT("hlen error: %d", count);
            return;
        }

        rc.hset(key, "f1", "2");
        rc.hset(key, "f2", "7");
        count = rc.hlen(key);
        if (count != 2)
        {
            ERROR_PRINT("hlen error: %d", count);
            return;
        }

        // field exists
        int_value = rc.hincrby(key, "f2", 3);
        if (int_value != 10)
        {
            ERROR_PRINT("hincrby error: %" PRId64, int_value);
            return;
        }
        rc.hget(key, "f2", &str_value);
        if (str_value != "10")
        {
            ERROR_PRINT("hincrby error: %s", str_value.c_str());
            return;
        }

        // field not exists
        int_value = rc.hincrby(key, "f3", 3);
        if (int_value != 3)
        {
            ERROR_PRINT("hincrby error: %" PRId64, int_value);
            return;
        }
        rc.hget(key, "f3", &str_value);
        if (str_value != "3")
        {
            ERROR_PRINT("hincrby error: %s", str_value.c_str());
            return;
        }
        count = rc.hlen(key);
        if (count != 3)
        {
            ERROR_PRINT("hlen error: %d", count);
            return;
        }

        rc.del(key);
        SUCCESS_PRINT("%s", "OK");
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

void test_hmincrby(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        const std::string key = "r3c_kk";
        rc.del(key);

        std::vector<std::pair<std::string, int64_t> > increments1(3);
        increments1[0].first = "f1";
        increments1[0].second = 3;
        increments1[1].first = "f2";
        increments1[1].second = 4;
        increments1[2].first = "f3";
        increments1[2].second = 5;
        rc.hmincrby(key, increments1);

        std::vector<std::string> fields(3);
        std::map<std::string, std::string> map1;
        fields[0] = "f1";
        fields[1] = "f2";
        fields[2] = "f3";
        int n = rc.hmget(key, fields, &map1);
        printf("n=%d\n", n);
        if ((map1["f1"] != "3") || (map1["f2"] != "4") || (map1["f3"] != "5"))
        {
            ERROR_PRINT("hincrby ERROR1: %s, %s, %s", map1["f1"].c_str(), map1["f2"].c_str(), map1["f3"].c_str());
            rc.del(key);
            return;
        }

        std::vector<std::pair<std::string, int64_t> > increments2(2);
        increments2[0].first = "f1";
        increments2[0].second = 1;
        increments2[1].first = "f3";
        increments2[1].second = 1;
        rc.hmincrby(key, increments2);

        std::map<std::string, std::string> map2;
        n = rc.hmget(key, fields, &map2);
        printf("n=%d\n", n);
        if ((map2["f1"] != "4") || (map2["f2"] != "4") || (map2["f3"] != "6"))
        {
            ERROR_PRINT("hincrby ERROR2: %s, %s, %s", map2["f1"].c_str(), map2["f2"].c_str(), map2["f3"].c_str());
            rc.del(key);
            return;
        }

        rc.del(key);
        SUCCESS_PRINT("%s", "OK");
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

////////////////////////////////////////////////////////////////////////////
// SET
void test_set(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        const std::string key = "r3c_kk";
        std::vector<std::string> members;
        std::string member = "member";
        int count;

        rc.del(key);
        {
            if (rc.sismember(key, member))
            {
                ERROR_PRINT("%s", "is member");
                return;
            }

            count = rc.sadd(key, member);
            printf("sadd count: %d\n", count);
            if (!rc.sismember(key, member))
            {
                ERROR_PRINT("%s", "is not member");
                return;
            }
        }

        {
            if (rc.sismember(key, "tom"))
            {
                ERROR_PRINT("%s", "is member");
                return;
            }

            members.push_back("mike");
            members.push_back("tom");
            members.push_back("marry");
            count = rc.sadd(key, members);
            printf("sadd count: %d\n", count);

            if (!rc.sismember(key, "mike") ||
                !rc.sismember(key, "tom") ||
                !rc.sismember(key, "marry") ||
                !rc.sismember(key, "member"))
            {
                ERROR_PRINT("%s", "is not member");
                return;
            }

            count = rc.smembers(key, &members);
            printf("smembers count: %d\n", count);
            if (members.size() != 4)
            {
                ERROR_PRINT("count error: %d\n", static_cast<int>(members.size()));
                return;
            }

            count = rc.scard(key);
            if (count != 4)
            {
                ERROR_PRINT("count error: %d\n", count);
                return;
            }

            count = rc.srandmember(key, 2, &members);
            if (count != 2)
            {
                ERROR_PRINT("count error: %d\n", count);
                return;
            }
            if (members.size() != 2)
            {
                ERROR_PRINT("count error: %d\n", static_cast<int>(members.size()));
                return;
            }

            if ((members[0] != "mike") &&
                (members[0] != "tom") &&
                (members[0] != "marry") &&
                (members[0] != "member") &&
                (members[1] != "mike") &&
                (members[1] != "tom") &&
                (members[1] != "marry") &&
                (members[1] != "member"))
            {
                ERROR_PRINT("member error: %s, %s", members[0].c_str(), members[1].c_str());
                return;
            }
        }

        {
            count = rc.spop(key, 2, &members);
            if ((count != 2) || (count != static_cast<int>(members.size())))
            {
                ERROR_PRINT("count error: %d, %d", count, static_cast<int>(members.size()));
                return;
            }

            if ((members[0] != "mike") &&
                (members[0] != "tom") &&
                (members[0] != "marry") &&
                (members[0] != "member") &&
                (members[1] != "mike") &&
                (members[1] != "tom") &&
                (members[1] != "marry") &&
                (members[1] != "member"))
            {
                ERROR_PRINT("member error: %s, %s", members[0].c_str(), members[1].c_str());
                return;
            }

            if (rc.sismember(key, members[1]) || rc.sismember(key, members[0]))
            {
                ERROR_PRINT("ismember error: %s, %s", members[0].c_str(), members[1].c_str());
                return;
            }
        }

        rc.del(key);
        SUCCESS_PRINT("%s", "OK");
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

////////////////////////////////////////////////////////////////////////////
// SORTED SET
void test_sorted_set(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        r3c::CRedisClient rc(redis_cluster_nodes);
        const std::string key = "r3c_kk";
        std::string field = "f 1";
        int64_t score = 0;
        int rank = 0;
        int count = 0;

        rc.del(key);
        rc.zadd(key, field, 3);
        printf("zadd %s ok\n", field.c_str());
        score = rc.zscore(key, field);
        if (score != 3)
        {
            ERROR_PRINT("score error: %" PRId64, score);
            return;
        }

        std::map<std::string, int64_t> map;
        map["f 2"] = 5;
        map["f 3"] = 7;
        count = rc.zadd(key, map);
        if (count != 2)
        {
            ERROR_PRINT("count error: %d", count);
            return;
        }
        score = rc.zscore(key, "f 3");
        if (score != 7)
        {
            ERROR_PRINT("score error: %" PRId64, score);
            return;
        }

        // zrank
        rank = rc.zrank(key, "f 3");
        if (rank != 2)
        {
            ERROR_PRINT("rank error: %d", rank);
            return;
        }
        rank = rc.zrank(key, "f 2");
        if (rank != 1)
        {
            ERROR_PRINT("rank error: %d", rank);
            return;
        }
        rank = rc.zrank(key, "f 1");
        if (rank != 0)
        {
            ERROR_PRINT("rank error: %d", rank);
            return;
        }
        rank = rc.zrank(key, "f X");
        if (rank != -1)
        {
            ERROR_PRINT("rank error: %d", rank);
            return;
        }

        // zrevrank
        rank = rc.zrevrank(key, "f 3");
        if (rank != 0)
        {
            ERROR_PRINT("rank error: %d", rank);
            return;
        }
        rank = rc.zrevrank(key, "f 2");
        if (rank != 1)
        {
            ERROR_PRINT("rank error: %d", rank);
            return;
        }
        rank = rc.zrevrank(key, "f 1");
        if (rank != 2)
        {
            ERROR_PRINT("rank error: %d", rank);
            return;
        }
        rank = rc.zrevrank(key, "f X");
        if (rank != -1)
        {
            ERROR_PRINT("rank error: %d", rank);
            return;
        }

        // zincrby
        score = rc.zincrby(key, "f 1", 7);
        if (score != 10)
        {
            ERROR_PRINT("score error: %" PRId64, score);
            return;
        }
        score = rc.zincrby(key, "f X", 6);
        if (score != 6)
        {
            ERROR_PRINT("score error: %" PRId64, score);
            return;
        }
        rank = rc.zrank(key, "f X");
        if (rank != 1)
        {
            ERROR_PRINT("rank error: %d", rank);
            return;
        }
        rank = rc.zrevrank(key, "f X");
        if (rank != 2)
        {
            ERROR_PRINT("rank error: %d", rank);
            return;
        }
        score = rc.zscore(key, "f X");
        if (score != 6)
        {
            ERROR_PRINT("score error: %" PRId64, score);
            return;
        }
        score = rc.zscore(key, "f 1");
        if (score != 3+7)
        {
            ERROR_PRINT("score error: %" PRId64, score);
            return;
        }

        // zadd
        rc.del(key);
        count = rc.zadd(key, "f1", 9);
        if (count != 1)
        {
            ERROR_PRINT("zadd error: %d", count);
            return;
        }
        count = rc.zadd(key, "f2", 2);
        if (count != 1)
        {
            ERROR_PRINT("zadd error: %d", count);
            return;
        }
        map.clear();
        map["f3\n"] = 7;
        map["f4"] = 3;
        map["f5 "] = 5;
        count = rc.zadd(key, map);
        if (count != 3)
        {
            ERROR_PRINT("zadd error: %d", count);
            return;
        }
        map["f5 "] = 6; // exists
        map["f6 "] = 2; // not exists
        count = rc.zadd(key, map);
        if (count != 1)
        {
            ERROR_PRINT("zadd error: %d", count);
            return;
        }

        // zcount
        count = rc.zcount(key, 0, 1);
        if (count != 0)
        {
            ERROR_PRINT("zcount error: %d", count);
            return;
        }
        count = rc.zcount(key, 0, 3);
        if (count != 3)
        {
            ERROR_PRINT("zcount error: %d", count);
            return;
        }
        count = rc.zcount(key, 0, 7);
        if (count != 5)
        {
            ERROR_PRINT("zcount error: %d", count);
            return;
        }

        // zscan
        std::vector<std::pair<std::string, int64_t> > values;
        count = rc.zscan(key, 0, &values);
        if ((count != 0) || static_cast<int>(values.size()) != 6)
        {
            ERROR_PRINT("zscan error: %d/%d", count, static_cast<int>(values.size()));
            return;
        }
        count = rc.zscan(key, 0, 3, &values);
        if ((count != 0) || static_cast<int>(values.size()) != 6)
        {
            ERROR_PRINT("zscan error: %d/%d", count, static_cast<int>(values.size()));
            return;
        }
        count = rc.zscan(key, 0, "f3", &values);
        if ((count != 0) || static_cast<int>(values.size()) != 0)
        {
            ERROR_PRINT("zscan error: %d/%d", count, static_cast<int>(values.size()));
            return;
        }
        count = rc.zscan(key, 0, "f2", &values);
        if ((count != 0) || static_cast<int>(values.size()) != 1)
        {
            ERROR_PRINT("zscan error: %d/%d", count, static_cast<int>(values.size()));
            return;
        }
        count = rc.zscan(key, 0, "f2", 1, &values);
        if ((count != 0) || static_cast<int>(values.size()) != 1)
        {
            ERROR_PRINT("zscan error: %d/%d", count, static_cast<int>(values.size()));
            return;
        }

        rc.del(key);
        SUCCESS_PRINT("%s", "OK");
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

void test_zrange(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        int count = -1;
        int64_t ret = 0;
        std::vector<std::pair<std::string, int64_t> > values;
        r3c::CRedisClient rc(redis_cluster_nodes);
        const std::string key = "r3c_kk";

        rc.del(key);
        ret = rc.zcard(key);
        if (ret != 0)
            ERROR_PRINT("zcard error: %" PRId64, ret);

        count = rc.zrange(key, 0, 100, false, &values);
        if (count != 0)
            ERROR_PRINT("zrange error: %d/%zd", count, values.size());
        count = -1;
        count = rc.zrange(key, 0, 100, true, &values);
        if (count != 0)
            ERROR_PRINT("zrange error: %d/%zd", count, values.size());

        rc.zadd(key, "f1", 2);
        rc.zadd(key, "f2", 1);
        rc.zadd(key, "f3", 5);
        rc.zadd(key, "f4", 6);
        rc.zadd(key, "f5", 7);
        rc.zadd(key, "f6", 4);
        rc.zadd(key, "f7", 5);

        ret = rc.zcard(key);
        if (ret != 7)
            ERROR_PRINT("zcard error: %" PRId64, ret);

        // without score
        count = rc.zrange(key, 0, 4, false, &values);
        if ((count != 5) || (values.size() != 5))
            ERROR_PRINT("zrange error: %d/%zd", count, values.size());
        if ((values[0].first != "f2") || (values[0].second != 0) ||
            (values[1].first != "f1") || (values[1].second != 0) ||
            (values[2].first != "f6") || (values[2].second != 0) ||
            (values[3].first != "f3") || (values[3].second != 0) ||
            (values[4].first != "f7") || (values[4].second != 0))
            ERROR_PRINT("zrange error: %s/%", values[0].first.c_str(), values[0].second);

        // with score
        values.clear();
        count = rc.zrange(key, 0, 4, true, &values);
        if ((count != 5) || (values.size() != 5))
            ERROR_PRINT("zrange error: %d/%zd", count, values.size());
        if ((values[0].first != "f2") || (values[0].second != 1) ||
            (values[1].first != "f1") || (values[1].second != 2) ||
            (values[2].first != "f6") || (values[2].second != 4) ||
            (values[3].first != "f3") || (values[3].second != 5) ||
            (values[4].first != "f7") || (values[4].second != 5))
            ERROR_PRINT("zrange error: %s/%", values[0].first.c_str(), values[0].second);

        // without score
        values.clear();
        count = rc.zrangebyscore(key, 2, 4, false, &values);
        if ((count != 2) || (values.size() != 2))
            ERROR_PRINT("zrangebyscore error: %d/%zd", count, values.size());
        if ((values[0].first != "f1") || (values[0].second != 0) ||
            (values[1].first != "f6") || (values[1].second != 0))
            ERROR_PRINT("zrangebyscore error: %s/%", values[0].first.c_str(), values[0].second);

        // with score
        values.clear();
        count = rc.zrangebyscore(key, 2, 4, true, &values);
        if ((count != 2) || (values.size() != 2))
            ERROR_PRINT("zrangebyscore error: %d/%zd", count, values.size());
        if ((values[0].first != "f1") || (values[0].second != 2) ||
            (values[1].first != "f6") || (values[1].second != 4))
            ERROR_PRINT("zrangebyscore error: %s/%", values[0].first.c_str(), values[0].second);

        rc.del(key);
        SUCCESS_PRINT("%s", "OK");
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

void test_zrevrange(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        int count = -1;
        std::vector<std::pair<std::string, int64_t> > values;
        r3c::CRedisClient rc(redis_cluster_nodes);
        const std::string key = "r3c_kk";

        rc.del(key);
        count = rc.zrevrange(key, 0, 100, false, &values);
        if (count != 0)
            ERROR_PRINT("zrevrange error: %d/%zd", count, values.size());
        count = -1;
        count = rc.zrevrange(key, 0, 100, true, &values);
        if (count != 0)
            ERROR_PRINT("zrevrange error: %d/%zd", count, values.size());

        rc.zadd(key, "f1", 2);
        rc.zadd(key, "f2", 1);
        rc.zadd(key, "f3", 5);
        rc.zadd(key, "f4", 6);
        rc.zadd(key, "f5", 7);
        rc.zadd(key, "f6", 4);
        rc.zadd(key, "f7", 5);

        // without score
        count = rc.zrevrange(key, 0, 4, false, &values);
        if ((count != 5) || (values.size() != 5))
            ERROR_PRINT("zrevrange error: %d/%zd", count, values.size());
        if ((values[0].first != "f5") || (values[0].second != 0) ||
            (values[1].first != "f4") || (values[1].second != 0) ||
            ((values[2].first != "f3") && (values[2].first != "f7")) || (values[2].second != 0) ||
            ((values[3].first != "f7") && (values[3].first != "f3")) || (values[3].second != 0) ||
            (values[4].first != "f6") || (values[4].second != 0))
            ERROR_PRINT("zrevrange error: %s/%" PRId64",%s/%" PRId64",%s/%" PRId64",%s/%" PRId64",%s/%" PRId64,
                    values[0].first.c_str(), values[0].second,
                    values[1].first.c_str(), values[1].second,
                    values[2].first.c_str(), values[2].second,
                    values[3].first.c_str(), values[3].second,
                    values[4].first.c_str(), values[4].second);

        // with score
        values.clear();
        count = rc.zrevrange(key, 0, 4, true, &values);
        if ((count != 5) || (values.size() != 5))
            ERROR_PRINT("zrevrange error: %d/%zd", count, values.size());
        if ((values[0].first != "f5") || (values[0].second != 7) ||
            (values[1].first != "f4") || (values[1].second != 6) ||
            ((values[2].first != "f3") && (values[2].first != "f7")) || (values[2].second != 5) ||
            ((values[3].first != "f7") && (values[3].first != "f3")) || (values[3].second != 5) ||
            (values[4].first != "f6") || (values[4].second != 4))
            ERROR_PRINT("zrevrange error: %s/%" PRId64",%s/%" PRId64",%s/%" PRId64",%s/%" PRId64",%s/%" PRId64,
                    values[0].first.c_str(), values[0].second,
                    values[1].first.c_str(), values[1].second,
                    values[2].first.c_str(), values[2].second,
                    values[3].first.c_str(), values[3].second,
                    values[4].first.c_str(), values[4].second);

        // without score
        values.clear();
        count = rc.zrevrangebyscore(key, 2, 4, false, &values);
        if ((count != 2) || (values.size() != 2))
            ERROR_PRINT("zrevrangebyscore error: %d/%zd", count, values.size());
        if ((values[0].first != "f6") || (values[0].second != 0) ||
            (values[1].first != "f1") || (values[1].second != 0))
            ERROR_PRINT("zrevrangebyscore error: %s/%" PRId64, values[0].first.c_str(), values[0].second);

        // with score
        values.clear();
        count = rc.zrevrangebyscore(key, 2, 4, true, &values);
        if ((count != 2) || (values.size() != 2))
            ERROR_PRINT("zrevrangebyscore error: %d/%zd", count, values.size());
        if ((values[0].first != "f6") || (values[0].second != 4) ||
            (values[1].first != "f1") || (values[1].second != 2))
            ERROR_PRINT("zrevrangebyscore error: %s/%" PRId64",%s/%" PRId64, values[0].first.c_str(), values[0].second, values[1].first.c_str(), values[1].second);

        rc.del(key);
        SUCCESS_PRINT("%s", "OK");
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}

void test_zrem(const std::string& redis_cluster_nodes)
{
    TIPS_PRINT();

    try
    {
        int count = -1;
        r3c::CRedisClient rc(redis_cluster_nodes);
        const std::string key = "r3c_kk";
        rc.del(key);

        std::map<std::string, int64_t> map;
        map["a"] = 1;
        map["b"] = 2;
        map["c"] = 3;
        map["d"] = 4;
        map["e"] = 5;
        count = rc.zadd(key, map);
        if (count != 5)
            ERROR_PRINT("zadd error: %d", count);

        count = rc.zrem(key, "c");
        if (count != 1)
            ERROR_PRINT("zrem error: %d", count);

        std::vector<std::string> fields;
        fields.push_back("x");
        fields.push_back("a");
        fields.push_back("e");
        count = rc.zrem(key, fields);
        if (count != 2)
            ERROR_PRINT("zrem error: %d", count);

        SUCCESS_PRINT("%s", "OK");
    }
    catch (r3c::CRedisException& ex)
    {
        ERROR_PRINT("ERROR: %s", ex.str().c_str());
    }
}
