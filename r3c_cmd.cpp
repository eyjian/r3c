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
#include "r3c.h"
#include <inttypes.h>
#include <stdlib.h>
#include <strings.h>

static void my_log_write(const char* format, ...)
{
    va_list ap;
    va_start(ap, format);
    vprintf(format, ap);
    va_end(ap);
}

int main(int argc, char* argv[])
{
    if (argc < 2)
    {
        fprintf(stderr, "Usage: r3c_cmd command parameter ...\n");
        exit(1);
    }

	r3c::set_info_log_write(my_log_write);
	r3c::set_debug_log_write(my_log_write);

    try
    {
        const char* cmd = argv[1];
        const char* key = argv[2];
        const char* nodes = getenv("HOSTS");
        if (NULL == nodes)
        {
            fprintf(stderr, "Environment[HOSTS] not set, example: export HOSTS=127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381\n");
            exit(1);
        }
        if ('\0' == *nodes)
        {
            fprintf(stderr, "Environment[HOSTS] without value, example: export HOSTS=127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381\n");
            exit(1);
        }

        int i = 0;
        int count = 0;
        int end = 0;
        int start = 0;
        int64_t score;
        int64_t new_score;
        int64_t min;
        int64_t max;
        int64_t increment;
        int64_t new_value_int64;
        int64_t result_int64;
        uint32_t seconds;
        std::string value;
        std::vector<std::string> fields;
        std::vector<std::string> values;
        std::map<std::string, std::string> map;
        std::map<std::string, std::string>::iterator iter;
        std::vector<std::string> vec;
        std::pair<std::string, uint16_t> which_node;
        r3c::CRedisClient redis_client(nodes);

        if (0 == strcasecmp(cmd, "list"))
        {
            std::vector<struct r3c::NodeInfo> nodes_info;
            int num_nodes = redis_client.list_nodes(&nodes_info, &which_node);

            fprintf(stdout, "number of nodes (from %s:%d): %d\n", which_node.first.c_str(), which_node.second, num_nodes);
            for (std::vector<struct r3c::NodeInfo>::size_type i=0; i<nodes_info.size(); ++i)
                std::cout << "[" << i << "] " << nodes_info[i] << std::endl;
        }
        ////////////////////////////////////////////////////////////////////////////
        // KEY VALUE
        else if (0 == strcasecmp(cmd, "del"))
        {
            if (!redis_client.del(key, &which_node))
                fprintf(stderr, "[%s] not exist\n", key);
            else
                fprintf(stdout, "[%s] deleted\n", key);
        }
        else if (0 == strcasecmp(cmd, "exists"))
        {
            // EXISTS command
            if (argc != 3)
            {
                fprintf(stderr, "Usage: r3c_cmd exists key\n");
                exit(1);
            }

            if (redis_client.exists(key, &which_node))
                fprintf(stdout, "[%s] exist\n", key);
            else
                fprintf(stderr, "[%s] not exist\n", key);
        }
        else if (0 == strcasecmp(cmd, "expire"))
        {
            // EXPIRE command
            if (argc != 4)
            {
                fprintf(stderr, "Usage: r3c_cmd expire key seconds\n");
                exit(1);
            }

            seconds = atoi(argv[3]);
            if (redis_client.expire(key, seconds, &which_node))
                fprintf(stdout, "[%s] exist\n", key);
            else
                fprintf(stderr, "[%s] not exist\n", key);
        }
        else if (0 == strcasecmp(cmd, "set"))
        {
            // SET command
            if (argc != 4)
            {
                fprintf(stderr, "Usage: r3c_cmd set key value\n");
                exit(1);
            }

            redis_client.set(key, argv[3], &which_node);
        }
        else if (0 == strcasecmp(cmd, "setnx"))
        {
            // SETNX command
            if (argc != 4)
            {
                fprintf(stderr, "Usage: r3c_cmd setnx key value\n");
                exit(1);
            }

            if (redis_client.setnx(key, argv[3], &which_node))
                fprintf(stdout, "[%s] ok\n", key);
            else
                fprintf(stderr, "[%s] exists\n", key);
        }
        else if (0 == strcasecmp(cmd, "setex"))
        {
            // SETEX command
            if (argc != 5)
            {
                fprintf(stderr, "Usage: r3c_cmd setex key seconds value\n");
                exit(1);
            }

            seconds = atoi(argv[3]);
            redis_client.setex(key, argv[4], seconds, &which_node);
            fprintf(stdout, "[%s] ok\n", key);
        }
        else if (0 == strcasecmp(cmd, "get"))
        {
            // GET command
            if (argc != 3)
            {
                fprintf(stderr, "Usage: r3c_cmd get key\n");
                exit(1);
            }

            if (!redis_client.get(key, &value, &which_node))
            {
                fprintf(stderr, "[%s] not exist\n", key);
            }
            else
            {
                fprintf(stdout, "[%s] => %s\n", key, value.c_str());
            }
        }
        else if (0 == strcasecmp(cmd, "incrby"))
        {
            // INCRBY command
            if (argc != 4)
            {
                fprintf(stderr, "Usage: r3c_cmd incrby key increment\n");
                exit(1);
            }

            increment = atoll(argv[3]);
            new_value_int64 = redis_client.incrby(key, increment, &which_node);
            fprintf(stdout, "%"PRId64"\n", new_value_int64);
        }
        ////////////////////////////////////////////////////////////////////////////
        // LIST
        else if (0 == strcasecmp(cmd, "llen"))
        {
            // LLEN command
            if (argc != 3)
            {
                fprintf(stderr, "Usage: r3c_cmd llen key\n");
                exit(1);
            }

            result_int64 = redis_client.llen(key, &which_node);
            fprintf(stdout, "[%s] => %"PRId64"\n", key, result_int64);
        }
        else if (0 == strcasecmp(cmd, "lpop"))
        {
            // LPOP command
            if (argc != 3)
            {
                fprintf(stderr, "Usage: r3c_cmd lpop key\n");
                exit(1);
            }

            redis_client.lpop(key, &value, &which_node);
            fprintf(stdout, "[%s] => %s\n", key, value.c_str());
        }
        else if (0 == strcasecmp(cmd, "lpush"))
        {
            // LPUSH command
            if (argc < 4)
            {
                fprintf(stderr, "Usage: r3c_cmd lpush key value1 value2 ...\n");
                exit(1);
            }

            values.clear();
            for (i=3; i<argc; ++i)
                values.push_back(argv[i]);
            count = redis_client.lpush(key, values, &which_node);
            fprintf(stdout, "[%s] => %d\n", key, count);
        }
        else if (0 == strcasecmp(cmd, "lrange"))
        {
            // LRANGE command
            if (argc != 5)
            {
                fprintf(stderr, "Usage: r3c_cmd lrange key start end\n");
                exit(1);
            }

            end = atoi(argv[4]);
            start = atoi(argv[3]);
            count = redis_client.lrange(key, start, end, &values, &which_node);
            fprintf(stdout, "count: %d\n", count);
            for (i=0; i<count; ++i)
                fprintf(stdout, "%s\n", values[i].c_str());
        }
        else if (0 == strcasecmp(cmd, "ltrim"))
        {
            // LTRIM command
            if (argc != 5)
            {
                fprintf(stderr, "Usage: r3c_cmd ltrim key start end\n");
                exit(1);
            }

            end = atoi(argv[4]);
            start = atoi(argv[3]);
            if (redis_client.ltrim(key, start, end, &which_node))
                fprintf(stdout, "OK\n");
            else
                fprintf(stderr, "ERROR\n");
        }
        else if (0 == strcasecmp(cmd, "rpop"))
        {
            // RPOP command
            if (argc != 3)
            {
                fprintf(stderr, "Usage: r3c_cmd rpop key\n");
                exit(1);
            }

            if (redis_client.rpop(key, &value, &which_node))
                fprintf(stdout, "%s\n", value.c_str());
            else
                fprintf(stderr, "ERROR\n");
        }
        else if (0 == strcasecmp(cmd, "rpush"))
        {
            // RPUSH command
            if (argc < 4)
            {
                fprintf(stderr, "Usage: r3c_cmd rpush key value1 value2 ...\n");
                exit(1);
            }

            if (4 == argc)
            {
                count = redis_client.rpush(key, argv[3], &which_node);
            }
            else
            {
                values.clear();
                for (i=3; i<argc; ++i)
                    values.push_back(argv[i]);
                count = redis_client.rpush(key, values, &which_node);
            }
            fprintf(stdout, "[%s] => %d\n", key, count);
        }
        else if (0 == strcasecmp(cmd, "rpushx"))
        {
            // RPUSHX command
            if (argc != 4)
            {
                fprintf(stderr, "Usage: r3c_cmd rpushx key value\n");
                exit(1);
            }

            count = redis_client.rpushx(key, argv[3], &which_node);
            fprintf(stdout, "[%s] => %d\n", key, count);
        }
        ////////////////////////////////////////////////////////////////////////////
        // HASH
        else if (0 == strcasecmp(cmd, "hdel"))
        {
            // HDEL command
            if (argc < 4)
            {
                fprintf(stderr, "Usage: r3c_cmd hdel key field1 field2 ...\n");
                exit(1);
            }

            if (4 == argc)
            {
                if (redis_client.hdel(key, argv[3], &which_node))
                    fprintf(stdout, "[%s] deleted\n", key);
                else
                    fprintf(stderr, "[%s] not exists\n", key);
            }
            else
            {
                std::vector<std::string> fields;
                for (i=3; i<argc; ++i)
                    fields.push_back(argv[i]);
                count = redis_client.hdel(key, fields, &which_node);
                if (count > 0)
                    fprintf(stdout, "[%s] deleted: %d\n", key, count);
                else
                    fprintf(stderr, "[%s] not exists\n", key);
            }
        }
        else if (0 == strcasecmp(cmd, "hexists"))
        {
            // HEXISTS command
            if (argc != 4)
            {
                fprintf(stderr, "Usage: r3c_cmd hexists key field\n");
                exit(1);
            }

            if (redis_client.hexists(key, argv[3], &which_node))
                fprintf(stdout, "[%s:%s] exist\n", key, argv[3]);
            else
                fprintf(stderr, "[%s:%s] not exist\n", key, argv[3]);
        }
        else if (0 == strcasecmp(cmd, "hlen"))
        {
            // HLEN command
            if (argc != 3)
            {
                fprintf(stderr, "Usage: r3c_cmd hlen key\n");
                exit(1);
            }

            new_value_int64 = redis_client.hlen(key, &which_node);
            fprintf(stdout, "[%s] fields count: %"PRId64"\n", key, new_value_int64);
        }
        else if (0 == strcasecmp(cmd, "hset"))
        {
            // HSET command
            if (argc != 5)
            {
                fprintf(stderr, "Usage: r3c_cmd hset key field value\n");
                exit(1);
            }

            redis_client.hset(key, argv[3], argv[4], &which_node);
        }
        else if (0 == strcasecmp(cmd, "hsetnx"))
        {
            // HSETNX command
            if (argc != 5)
            {
                fprintf(stderr, "Usage: r3c_cmd hsetnx key field value\n");
                exit(1);
            }

            if (redis_client.hsetnx(key, argv[3], argv[4], &which_node))
                fprintf(stdout, "[%s:%s] ok\n", key, argv[3]);
            else
                fprintf(stderr, "[%s:%s] exists\n", key, argv[3]);
        }
        else if (0 == strcasecmp(cmd, "hget"))
        {
            // HGET command
            if (argc != 4)
            {
                fprintf(stderr, "Usage: r3c_cmd hget key field\n");
                exit(1);
            }

            if (!redis_client.hget(key, argv[3], &value, &which_node))
            {
                fprintf(stdout, "[%s:%s] not exist\n", key, argv[3]);
            }
            else
            {
                fprintf(stdout, "[%s:%s] => %s\n", key, argv[3], value.c_str());
            }
        }
        else if (0 == strcasecmp(cmd, "hincrby"))
        {
            // HINCRBY command
            if (argc != 5)
            {
                fprintf(stderr, "Usage: r3c_cmd hincrby key field increment\n");
                exit(1);
            }

            increment = atoll(argv[4]);
            new_value_int64 = redis_client.hincrby(key, argv[3], increment, &which_node);
            fprintf(stdout, "%"PRId64"\n", new_value_int64);
        }
        else if (0 == strcasecmp(cmd, "hmset"))
        {
            // HMSET command
            if ((argc < 5) || (argc % 2 != 1))
            {
                fprintf(stderr, "Usage: r3c_cmd hmset key field1 value1 field2 value2 ...\n");
                exit(1);
            }

            for (i=3; i<argc; i+=2)
                map[argv[i]] = argv[i+1];
            redis_client.hmset(key, map, &which_node);
        }
        else if (0 == strcasecmp(cmd, "hmget"))
        {
            // HMGET command
            if (argc < 4)
            {
                fprintf(stderr, "Usage: r3c_cmd hmget key field1 field2 ...\n");
                exit(1);
            }

            for (i=3; i<argc; ++i)
                vec.push_back(argv[i]);
            redis_client.hmget(key, vec, &map, false, &which_node);
            if (map.empty())
                fprintf(stdout, "not exists\n");
            else
                for (iter=map.begin(); iter!=map.end(); ++iter)
                    fprintf(stdout, "%s => %s\n", iter->first.c_str(), iter->second.c_str());
        }
        else if (0 == strcasecmp(cmd, "hgetall"))
        {
            // HGETALL command
            if (argc != 3)
            {
                fprintf(stderr, "Usage: r3c_cmd hgetall key\n");
                exit(1);
            }

            count = redis_client.hgetall(key, &map, &which_node);
            if (0 == count)
            {
                fprintf(stderr, "[%s] not exists\n", key);
            }
            else
            {
                for (iter=map.begin(); iter!=map.end(); ++iter)
                    fprintf(stdout, "%s => %s\n", iter->first.c_str(), iter->second.c_str());
            }
        }
        else if (0 == strcasecmp(cmd, "hkeys"))
        {
            // HKEYS command
            if (argc != 3)
            {
                fprintf(stderr, "Usage: r3c_cmd hkeys key\n");
                exit(1);
            }

            count = redis_client.hkeys(key, &fields, &which_node);
            if (0 == count)
            {
                fprintf(stderr, "[%s] not exists\n", key);
            }
            else
            {
                for (std::vector<std::string>::size_type k=0; k<fields.size(); ++k)
                    fprintf(stdout, "%s\n", fields[k].c_str());
            }
        }
        else if (0 == strcasecmp(cmd, "hvals"))
        {
            // HVALS command
            if (argc != 3)
            {
                fprintf(stderr, "Usage: r3c_cmd hvals key\n");
                exit(1);
            }

            count = redis_client.hvals(key, &values, &which_node);
            if (0 == count)
            {
                fprintf(stderr, "[%s] not exists\n", key);
            }
            else
            {
                for (std::vector<std::string>::size_type k=0; k<values.size(); ++k)
                    fprintf(stdout, "%s\n", values[k].c_str());
            }
        }
        ////////////////////////////////////////////////////////////////////////////
        // SET

        ////////////////////////////////////////////////////////////////////////////
        // SORTED SET
        else if (0 == strcasecmp(cmd, "zadd"))
        {
            // ZADD command
            if (argc != 5)
            {
                fprintf(stderr, "Usage: r3c_cmd zadd key field score\n");
                exit(1);
            }

            score = atoll(argv[4]);
            new_score = redis_client.zadd(key, argv[3], score, &which_node);
            if (new_score < score)
            {
                fprintf(stderr, "[%s] less\n", key);
            }
            else
            {
                fprintf(stdout, "%s:%s => %"PRId64"\n", key, argv[3], new_score);
            }
        }
        else if (0 == strcasecmp(cmd, "zcount"))
        {
            // ZCOUNT command
            if (argc != 5)
            {
                fprintf(stderr, "Usage: r3c_cmd zcount key min max\n");
                exit(1);
            }

            min = atoll(argv[3]);
            max = atoll(argv[4]);
            count = redis_client.zcount(key, min, max, &which_node);
            fprintf(stdout, "%d\n", count);
        }
        else
        {
            fprintf(stderr, "command[%s] not supported\n", cmd);
            exit(1);
        }
    }
    catch (r3c::CRedisException& ex)
    {
        fprintf(stderr, "\033[0;32;31m%s\033[m\n", ex.str().c_str());
        exit(1);
    }

    return 0;
}
