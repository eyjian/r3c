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
#include "sha1.h"
#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define R3C_ASSERT(x) assert(x)

#ifdef __GNUC__
#  define UNUSED(x) UNUSED_ ## x __attribute__((__unused__))
#else
#  define UNUSED(x) UNUSED_ ## x
#endif

#define THROW_REDIS_EXCEPTION(errcode, errmsg) throw CRedisException(errcode, errmsg, __FILE__, __LINE__)
#define THROW_REDIS_EXCEPTION_WITH_NODE(errcode, errmsg, node_ip, node_port) throw CRedisException(errcode, errmsg, __FILE__, __LINE__, node_ip, node_port, NULL, NULL)
#define THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(errcode, errmsg, node_ip, node_port, command, key) throw CRedisException(errcode, errmsg, __FILE__, __LINE__, node_ip, node_port, command, key)

////////////////////////////////////////////////////////////////////////////////
std::ostream& operator <<(std::ostream& os, const struct redisReply& redis_reply)
{
    if (REDIS_REPLY_STRING == redis_reply.type)
    {
        os << "type: string" << std::endl
           << redis_reply.str << std::endl;
    }
    else if (REDIS_REPLY_ARRAY == redis_reply.type)
    {
        os << "type: array" << std::endl;
    }
    else if (REDIS_REPLY_INTEGER == redis_reply.type)
    {
        os << "type: integer" << std::endl
           << redis_reply.integer << std::endl;
    }
    else if (REDIS_REPLY_NIL == redis_reply.type)
    {
        os << "type: nil" << std::endl;
    }
    else if (REDIS_REPLY_STATUS == redis_reply.type)
    {
        os << "type: status" << std::endl
           << redis_reply.integer << std::endl;
    }
    else if (REDIS_REPLY_ERROR == redis_reply.type)
    {
        os << "type: error" << std::endl
           << redis_reply.str << std::endl;
    }
    else
    {
        os << "type: unknown" << std::endl;
    }

    return os;
}

namespace r3c {

enum
{
    CLUSTER_SLOTS = 16384 // number of slots, defined in cluster.h
};

// help to release va_list
struct va_list_helper
{
    va_list_helper(va_list& ap)
        : _ap(ap)
    {
    }

    ~va_list_helper()
    {
        va_end(_ap);
    }

    va_list& _ap;
};

/*
 * Implemented in crc16.c
 */
extern uint16_t crc16(const char *buf, int len);

/* Copy from cluster.c
 *
 * We have 16384 hash slots. The hash slot of a given key is obtained
 * as the least significant 14 bits of the crc16 of the key.
 *
 * However if the key contains the {...} pattern, only the part between
 * { and } is hashed. This may be useful in the future to force certain
 * keys to be in the same node (assuming no resharding is in progress). */
static unsigned int keyHashSlot(const char *key, size_t keylen) {
    size_t s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return crc16(key,keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s+1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing betweeen {} ? Hash the whole key. */
    if (e == keylen || e == s+1) return crc16(key,keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
     * what is in the middle between { and }. */
    return crc16(key+s+1,e-s-1) & 0x3FFF; // 0x3FFF == 16383
}

static bool parse_node_string(const std::string node_string, std::string* ip, uint16_t* port)
{
    std::string::size_type colon_pos = node_string.find(':');
    if (colon_pos == std::string::npos)
    {
        return false;
    }
    else
    {
        const std::string port_str = node_string.substr(colon_pos+1);
        *port = atoi(port_str.c_str());
        *ip = node_string.substr(0, colon_pos);
        return true;
    }
}

static void parse_slot_string(const std::string slot_string, int* start_slot, int* end_slot)
{
    std::string::size_type bar_pos = slot_string.find('-');
    if (bar_pos == std::string::npos)
    {
        *start_slot = atoi(slot_string.c_str());
        *end_slot = *start_slot;
    }
    else
    {
        const std::string end_slot_str = slot_string.substr(bar_pos+1);
        *end_slot = atoi(end_slot_str.c_str());
        *start_slot = atoi(slot_string.substr(0, bar_pos).c_str());
    }
}

std::string ip2string(uint32_t ip)
{
    struct in_addr in;
    in.s_addr = ip;
    return inet_ntoa(in);
}

std::string strsha1(const std::string& str)
{
    static unsigned char hex_table[16] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    std::string result(40, '\0'); // f3512504d8a2f422b45faad2f2f44d569a963da1
    unsigned char hash[20];
    SHA1_CTX ctx;

    SHA1Init(&ctx);
    SHA1Update(&ctx, (const unsigned char*)str.data(), str.size());
    SHA1Final(hash, &ctx);

    for (size_t i=0,j=0; i<sizeof(hash)/sizeof(hash[0]); ++i,j+=2)
    {
        result[j] = hex_table[(hash[i] >> 4) & 0x0f];
        result[j+1] = hex_table[hash[i] & 0x0f];
    }

    return result;
}

int split(std::vector<std::string>* tokens, const std::string& source, const std::string& sep, bool skip_sep)
{
    if (sep.empty())
    {
        tokens->push_back(source);
    }
    else if (!source.empty())
    {
        std::string str = source;
        std::string::size_type pos = str.find(sep);

        while (true)
        {
            std::string token = str.substr(0, pos);
            tokens->push_back(token);

            if (std::string::npos == pos)
            {
                break;
            }
            if (skip_sep)
            {
                bool end = false;
                while (0 == strncmp(sep.c_str(), &str[pos+1], sep.size()))
                {
                    pos += sep.size();
                    if (pos >= str.size())
                    {
                        end = true;
                        tokens->push_back(std::string(""));
                        break;
                    }
                }

                if (end)
                    break;
            }

            str = str.substr(pos + sep.size());
            pos = str.find(sep);
        }
    }

    return static_cast<int>(tokens->size());
}

void millisleep(uint32_t millisecond)
{
    struct timespec ts = { millisecond / 1000, (millisecond % 1000) * 1000000 };
    while ((-1 == nanosleep(&ts, &ts)) && (EINTR == errno));
}

void free_redis_reply(const redisReply* redis_reply)
{
    freeReplyObject((void*)redis_reply);
}

unsigned int get_key_slot(const std::string* key)
{
    if (key != NULL)
    {
        return keyHashSlot(key->c_str(), key->size());
    }
    else
    {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        srandom(tv.tv_usec);
        return random() & 0x3FFF;
    }
}

std::string format_string(const char* format, ...)
{
    va_list ap;
    size_t size = getpagesize();
    char* buffer = new char[size];

    while (true)
    {
        va_start(ap, format);
        int expected = vsnprintf(buffer, size, format, ap);

        va_end(ap);
        if (expected > -1 && expected < (int)size)
            break;

        /* Else try again with more space. */
        if (expected > -1)    /* glibc 2.1 */
            size = (size_t)expected + 1; /* precisely what is needed */
        else           /* glibc 2.0 */
            size *= 2;  /* twice the old size */

        delete []buffer;
        buffer = new char[size];
    }

    std::string str = buffer;
    delete []buffer;
    return str;
}

////////////////////////////////////////////////////////////////////////////////
static void null_log_write(const char* UNUSED(format), ...)
{
}

static void r3c_log_write(const char* format, ...)
{
    va_list ap;
    va_start(ap, format);
    vprintf(format, ap);
    va_end(ap);
}

static LOG_WRITE g_error_log = r3c_log_write;
static LOG_WRITE g_info_log = r3c_log_write;
static LOG_WRITE g_debug_log = r3c_log_write;

void set_error_log_write(LOG_WRITE info_log)
{
    g_error_log = info_log;
    if (NULL == g_error_log)
        g_error_log = null_log_write;
}

void set_info_log_write(LOG_WRITE info_log)
{
    g_info_log = info_log;
    if (NULL == g_info_log)
        g_info_log = null_log_write;
}

void set_debug_log_write(LOG_WRITE debug_log)
{
    g_debug_log = debug_log;
    if (NULL == g_debug_log)
        g_debug_log = null_log_write;
}

class FreeArgvHelper
{
public:
    FreeArgvHelper(int argc, char* argv[], size_t* argv_len)
        : _argc(argc), _argv(argv), _argv_len(argv_len)
    {
    }

    ~FreeArgvHelper()
    {
        for (int i=0; i<_argc; ++i)
            delete []_argv[i];

        delete []_argv;
        delete []_argv_len;
    }

private:
    int _argc;
    char** _argv;
    size_t* _argv_len;
};

static void print_reply(const char* command, const char* key, unsigned int slot, redisReply* redis_reply, int excepted_reply_type, const std::pair<std::string, uint16_t>& node)
{
    if (REDIS_REPLY_STRING == redis_reply->type)
        (*g_debug_log)("[%s:%d][STRING][%s][SLOT:%u]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d/%d)%s\n", __FILE__, __LINE__, command, slot, key, node.first.c_str(), node.second, redis_reply->type, excepted_reply_type, redis_reply->str);
    else if (REDIS_REPLY_INTEGER == redis_reply->type)
        (*g_debug_log)("[%s:%d][INTEGER][%s][SLOT:%u]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d/%d)%lld\n", __FILE__, __LINE__, command, slot, key, node.first.c_str(), node.second, redis_reply->type, excepted_reply_type, redis_reply->integer);
    else if (REDIS_REPLY_ARRAY == redis_reply->type)
        (*g_debug_log)("[%s:%d][ARRAY][%s][SLOT:%u]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d/%d)%zd\n", __FILE__, __LINE__, command, slot, key, node.first.c_str(), node.second, redis_reply->type, excepted_reply_type, redis_reply->elements);
    else if (REDIS_REPLY_NIL == redis_reply->type)
        (*g_debug_log)("[%s:%d][NIL][%s][SLOT:%u]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d/%d)%s\n", __FILE__, __LINE__, command, slot, key, node.first.c_str(), node.second, redis_reply->type, excepted_reply_type, redis_reply->str);
    else if (REDIS_REPLY_ERROR == redis_reply->type)
        (*g_debug_log)("[%s:%d][ERROR][%s][SLOT:%u]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d/%d)(%d)%s\n", __FILE__, __LINE__, command, slot, key, node.first.c_str(), node.second, redis_reply->type, excepted_reply_type, redis_reply->integer, redis_reply->str);
    else if (REDIS_REPLY_STATUS == redis_reply->type)
        (*g_debug_log)("[%s:%d][STATUS][%s][SLOT:%u]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d/%d)%s\n", __FILE__, __LINE__, command, slot, key, node.first.c_str(), node.second, redis_reply->type, excepted_reply_type, redis_reply->str);
    else
        (*g_debug_log)("[%s:%d][->%d][%s][SLOT:%u]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d/%d)%s\n", __FILE__, __LINE__, redis_reply->type, command, slot, key, node.first.c_str(), node.second, redis_reply->type, excepted_reply_type, redis_reply->str);
}

std::ostream& operator <<(std::ostream& os, const struct NodeInfo& node_info)
{
    os << node_info.id << " " << node_info.ip << ":" << node_info.port << " " << node_info.flags << " "
       << node_info.master_id << " " << node_info.ping_sent << " " << node_info.pong_recv << " " << node_info.epoch << " ";

    if (node_info.connected)
        os << "connected" << " ";
    else
        os << "disconnected" << " ";

    for (std::vector<std::pair<int, int> >::size_type i=0; i<node_info.slots.size(); ++i)
    {
        if (node_info.slots[i].first == node_info.slots[i].second)
            os << node_info.slots[i].first;
        else
            os << node_info.slots[i].first << "-" << node_info.slots[i].second;
    }

    return os;
}

////////////////////////////////////////////////////////////////////////////////
CRedisException::CRedisException(int errcode, const std::string& errmsg, const char* file, int line, const std::string& node_ip, uint16_t node_port, const char* command, const char* key) throw ()
    : _errcode(errcode), _errmsg(errmsg), _file(file), _line(line), _node_ip(node_ip), _node_port(node_port)
{
    if (command != NULL)
        _command = command;
    if (key != NULL)
        _key = key;
}

const char* CRedisException::what() const throw()
{
    return _errmsg.c_str();
}

std::string CRedisException::str() const throw ()
{
    return format_string("redis://%s:%d/%s/%s/%d:%s@%s:%d", _node_ip.c_str(), _node_port, _command.c_str(), _key.c_str(), _errcode, _errmsg.c_str(), _file.c_str(), _line);
}

////////////////////////////////////////////////////////////////////////////////
struct SlotInfo
{
    int slot;
    redisContext* redis_context;
    std::pair<uint32_t, uint16_t> node;
};

struct ParamInfo
{
    const char* command;
    size_t command_length;
    const std::string* key;
    const std::string* str1;
    const std::string* str2;
    const std::string* str3;
    const std::string* str4;
    const std::string* str5;
    const std::vector<std::string>* array;
    const std::map<std::string, std::string>* in_map1;
    const std::map<std::string, int64_t>* in_map2;
    const std::string* str6;
    const std::string* str7;
    const char* tag;
    size_t tag_length;
    std::string* value;
    std::vector<std::string>* values;
    std::map<std::string, std::string>* out_map;
    std::vector<std::pair<std::string, int64_t> >* out_vec;
    const bool* keep_null;
    const bool* withscores;
    std::pair<std::string, uint16_t>* which;

    ParamInfo(const char* command_, size_t command_length_, const std::string* key_, std::pair<std::string, uint16_t>* which_)
        : command(command_), command_length(command_length_), key(key_),
          str1(NULL), str2(NULL), str3(NULL), str4(NULL), str5(NULL),
          array(NULL), in_map1(NULL), in_map2(NULL),
          str6(NULL), str7(NULL),
          tag(NULL), tag_length(0),
          value(NULL), values(NULL),
          out_map(NULL), out_vec(NULL),
          keep_null(NULL), withscores(NULL),
          which(which_)
    {
    }
};

CRedisClient::CRedisClient(const std::string& nodes, int connect_timeout_milliseconds, int data_timeout_milliseconds, const std::string& password) throw (CRedisException)
    : _cluster_mode(false), _password(password), _nodes_string(nodes),
      _connect_timeout_milliseconds(connect_timeout_milliseconds), _data_timeout_milliseconds(data_timeout_milliseconds),
      _retry_times(RETRY_TIMES), _retry_sleep_milliseconds(RETRY_SLEEP_MILLISECONDS),
      _redis_context(NULL), _slots(CLUSTER_SLOTS, NULL)
{
    parse_nodes();

    if (_nodes.empty())
    {
        THROW_REDIS_EXCEPTION(ERR_PARAMETER, "invalid nodes or not set");
    }
    else if (_nodes.size() > 1)
    {
        _cluster_mode = true;
        init();
    }
}

CRedisClient::~CRedisClient()
{
    clear();
}

bool CRedisClient::cluster_mode() const
{
    return _cluster_mode;
}

void CRedisClient::set_retry(int retry_times, int retry_sleep_milliseconds)
{
    _retry_times = retry_times;
    _retry_sleep_milliseconds = retry_sleep_milliseconds;

    if (_retry_times < 0)
        _retry_times = 0;
    if (_retry_sleep_milliseconds < 1)
        _retry_sleep_milliseconds = 1;
}

////////////////////////////////////////////////////////////////////////////////
int CRedisClient::list_nodes(std::vector<struct NodeInfo>* nodes_info, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    if (!_cluster_mode)
    {
        return 0;
    }
    else
    {
        int i = 0;
        int errcode = 0;
        int num_nodes = static_cast<int>(_nodes.size());
        std::string errmsg;
        std::pair<std::string, uint16_t> node;

        for (i=0; i<num_nodes; ++i)
        {
            redisReply* redis_reply = NULL;
            redisContext* redis_context = connect_node(&errcode, &errmsg, &node);
            if (NULL == redis_context)
            {
                THROW_REDIS_EXCEPTION_WITH_NODE(errcode, errmsg.c_str(), node.first, node.second);
            }
            else if (redis_context->err != 0)
            {
                errcode = redis_context->err;
                errmsg = redis_context->errstr;
                redisFree(redis_context);
                redis_context = NULL;

                (*g_error_log)("[%d][%s:%d](%d)%s\n", i, __FILE__, __LINE__, errcode, errmsg.c_str());
                continue;
            }

            (*g_debug_log)("[%d][%s:%d]connected %s:%d\n", i, __FILE__, __LINE__, node.first.c_str(), (int)node.second);
            if (which != NULL)
            {
                which->first = node.first;
                which->second = node.second;
            }

            redis_reply = (redisReply*)redisCommand(redis_context, "CLUSTER NODES");
            if (NULL == redis_reply)
            {
                errcode = ERROR_COMMAND;
                errmsg = "redis `CLUSTER NODES` error";
                (*g_error_log)("[%d][%s:%d][%s:%d](%d)%s|(%d)%s\n", i, __FILE__, __LINE__, node.first.c_str(), (int)node.second, errcode, errmsg.c_str(), redis_context->err, redis_context->errstr);

                redisFree(redis_context);
                redis_context = NULL;
                continue;
            }
            else if (redis_reply->type != REDIS_REPLY_STRING)
            {
                // LOADING Redis is loading the dataset in memory
                errcode = redis_reply->type;
                errmsg = redis_reply->str;
                (*g_error_log)("[%d/%d][%s:%d][%s:%d](%d)%s|(%d)%s\n", i, num_nodes, __FILE__, __LINE__, node.first.c_str(), (int)node.second, errcode, errmsg.c_str(), redis_context->err, redis_context->errstr);

                freeReplyObject(redis_reply);
                redisFree(redis_context);
                redis_reply = NULL;
                redis_context = NULL;

                //THROW_REDIS_EXCEPTION_WITH_NODE(errcode, errmsg.c_str(), node.first, node.second);
                continue;
            }

            /*
             * <id> <ip:port> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
             * flags: A list of comma separated flags: myself, master, slave, fail?, fail, handshake, noaddr, noflags
             * ping-sent: Milliseconds unix time at which the currently active ping was sent, or zero if there are no pending pings
             * pong-recv: Milliseconds unix time the last pong was received
             * link-state: The state of the link used for the node-to-node cluster bus. We use this link to communicate with the node. Can be connected or disconnected
             *
             * `redis_reply->str` example:
             * 56686c7baad565d4370b8f1f6518a67b6cedb210 10.225.168.52:6381 slave 150f77d1000003811fb3c38c3768526a0b25ec31 0 1464662426768 22 connected
             * 150f77d1000003811fb3c38c3768526a0b25ec31 10.225.168.51:6379 myself,master - 0 0 22 connected 3278-5687 11092-11958
             * 6a7709bc680f7b224d0d20bdf7dd14db1f013baf 10.212.2.71:6381 master - 0 1464662429775 24 connected 8795-10922 11959-13107
             */
            std::vector<std::string> lines;
            int num_lines = split(&lines, redis_reply->str, "\n");
            freeReplyObject(redis_reply);
            redisFree(redis_context);
            redis_reply = NULL;
            redis_context = NULL;
            errcode = 0; // Must reset to zero
            errmsg.clear();

            for (int row=0; row<num_lines; ++row)
            {
                std::vector<std::string> tokens;
                const std::string& line = lines[row];
                int num_tokens = split(&tokens, line, " ");
                if (num_tokens >= 8)
                {
                    NodeInfo node_info;
                    node_info.id = tokens[0];
                    if (!parse_node_string(tokens[1], &node_info.ip, &node_info.port))
                    {
                        (*g_debug_log)("[%s:%d][%s:%d]invalid node_string: %s\n", __FILE__, __LINE__, node.first.c_str(), (int)node.second, tokens[1].c_str());
                    }
                    else
                    {
                        std::string::size_type pos_fail = tokens[2].find("fail");
                        std::string::size_type pos_master = tokens[2].find("master");

                        node_info.flags = tokens[2];
                        node_info.is_fail = (pos_fail != std::string::npos);
                        node_info.is_master = (pos_master != std::string::npos);
                        node_info.master_id = tokens[3];
                        node_info.ping_sent = atoi(tokens[4].c_str());
                        node_info.pong_recv = atoi(tokens[5].c_str());
                        node_info.epoch = atoi(tokens[6].c_str());
                        node_info.connected = (tokens[7] == "connected");

                        if (node_info.is_fail)
                        {
                            //(*g_error_log)("[%s:%d]%s\n", __FILE__, __LINE__, node_info.str().c_str());
                        }
                        else
                        {
                            //(*g_debug_log)("[%s:%d]%s\n", __FILE__, __LINE__, node_info.str().c_str());

                            for (int col=8; col<num_tokens; ++col)
                            {
                                std::pair<int, int> slot;
                                parse_slot_string(tokens[col], &slot.first, &slot.second);
                                node_info.slots.push_back(slot);
                            }
                        }

                        nodes_info->push_back(node_info);
                    } // if (parse_node_string
                } // if (num_tokens >= 8)
            } // for (int row=0; row<num_lines; ++row)
            break;
        } // for

        //(*g_debug_log)("[%d/%d][%s:%d][%s:%d][%d]errcode=(%d)%s\n", i, num_nodes, __FILE__, __LINE__, node.first.c_str(), (int)node.second, static_cast<int>(nodes_info->size()), errcode, errmsg.c_str());
        if (errcode != 0)
            THROW_REDIS_EXCEPTION_WITH_NODE(errcode, errmsg.c_str(), node.first, node.second);
        return static_cast<int>(nodes_info->size());
    }
}

void CRedisClient::flushall(std::vector<std::pair<std::string, std::string> >* results) throw (CRedisException)
{
    std::vector<struct NodeInfo> nodes_info;
    list_nodes(&nodes_info);

    for (std::vector<struct NodeInfo>::size_type i=0; i<nodes_info.size(); ++i)
    {
        const struct NodeInfo& node_info = nodes_info[i];
        std::pair<std::string, std::string> pair;
        pair.first = node_info.ip + std::string(":") + any2string(node_info.port);

        if (!node_info.is_master)
        {
            pair.second = "IS NOT MASTER";
        }
        else
        {
            redisContext* redis_context = NULL;
            if (_connect_timeout_milliseconds <= 0)
            {
                redis_context = redisConnect(node_info.ip.c_str(), node_info.port);
				
            }
            else
            {
                struct timeval timeout;
                timeout.tv_sec = _connect_timeout_milliseconds / 1000;
                timeout.tv_usec = (_connect_timeout_milliseconds % 1000) * 1000;
                redis_context = redisConnectWithTimeout(node_info.ip.c_str(), node_info.port, timeout);
            }

			

            if (NULL == redis_context)
            {
                pair.second = "INVALID CONTEXT";
            }
            else
            {
				if(_password.size() > 0)
				{
					redisReply* redis_reply = (redisReply*)redisCommand(redis_context, "auth %s", _password.c_str());
					if (NULL == redis_reply)
					{
						(*g_error_log)("[%s:%d]auth failure\n", __FILE__, __LINE__);						
					}

				}
                           
               
                redisReply* redis_reply = (redisReply*)redisCommand(redis_context, "FLUSHALL");
                if (NULL == redis_reply)
                {
                    pair.second = "FLUSHALL ERROR";
                }
                else
                {
                    pair.second = redis_reply->str;
                    freeReplyObject(redis_reply);
                }

                redisFree(redis_context);
            }
        }

        if (results != NULL)
        {
            results->push_back(pair);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// KEY VALUE
bool CRedisClient::key_type(const std::string& key, std::string* key_type, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("TYPE", sizeof("TYPE")-1, &key, which);
    param_info.value = key_type;
    redis_command(REDIS_REPLY_STATUS, &param_info);
    return !key_type->empty();
}

#if 0
// binary unsafe, key and value can not contain space & LF etc.
bool CRedisClient::exists(const std::string& key, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    int64_t result = 0;

    const std::string command_string = format_string("EXISTS %s", key.c_str());
    const redisReply* redis_reply = redis_command(REDIS_REPLY_INTEGER, which, key, "EXISTS", command_string);
    if (redis_reply != NULL)
    {
        result = redis_reply->integer;
        freeReplyObject(const_cast<redisReply*>(redis_reply));
        redis_reply = NULL;
    }

    return result > 0;
}
#else
// binary safe, key and value can contain any.
bool CRedisClient::exists(const std::string& key, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
	struct ParamInfo param_info("EXISTS", sizeof("EXISTS")-1, &key, which);
	int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
	return result > 0;
}
#endif

bool CRedisClient::expire(const std::string& key, uint32_t seconds, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str6  = any2string(seconds);
    struct ParamInfo param_info("EXPIRE", sizeof("EXPIRE")-1, &key, which);
    param_info.str6 = &str6;

    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return result > 0;
}

// EVAL script numkeys key [key ...] arg [arg ...]
const RedisReplyHelper CRedisClient::eval(const std::string& key, const std::string& lua_scripts, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const int excepted_reply_type = -1;
    const int argc = 3;
    size_t* argv_len = new size_t[argc];
    char** argv = new char*[argc];

    argv_len[0] = sizeof("EVAL")-1;
    argv_len[1] = lua_scripts.size();
    argv_len[2] = 1;
    argv[0] = new char[argv_len[0]+1];
    argv[1] = new char[argv_len[1]+1];
    argv[2] = new char[argv_len[2]+1];
    strncpy(argv[0], "EVAL", sizeof("EVAL"));
    strncpy(argv[1], lua_scripts.c_str(), lua_scripts.size()+1);
    strncpy(argv[2], "0", 2);
    FreeArgvHelper fah(argc, argv, argv_len);

    const std::string command_string;
    const redisReply* redis_reply = redis_command(excepted_reply_type, which, &key, "EVAL", command_string, argc, (const char**)argv, argv_len);
    return RedisReplyHelper(redis_reply);
}

// EVAL script numkeys key [key ...] arg [arg ...]
// EVALSHA sha1 numkeys key [key ...] arg [arg ...]
//
// r3c_cmd eval 123456 "local n; n=redis.call('setnx',KEYS[1],ARGV[1]); if (n>0) then redis.call('expire', KEYS[1], ARGV[2]) end; return n;" abcdefg 10
const RedisReplyHelper CRedisClient::eval(const std::string& key, const std::string& lua_scripts, const std::vector<std::string>& parameters, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    return do_eval("EVAL", key, lua_scripts, parameters, which);
}

const RedisReplyHelper CRedisClient::evalsha(const std::string& key, const std::string& sha1, const std::vector<std::string>& parameters, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    return do_eval("EVALSHA", key, sha1, parameters, which);
}

const RedisReplyHelper CRedisClient::do_eval(const char* eval_command, const std::string& key, const std::string& lua_script_or_sha1, const std::vector<std::string>& parameters, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const int excepted_reply_type = -1;

    // 3: EVAL script 1 key
    const int argc = 4 + static_cast<int>(parameters.size());
    size_t* argv_len = new size_t[argc];
    char** argv = new char*[argc];
    int argv_len_index = 0;

    // EVAL/EVALSHA
    argv_len[argv_len_index] = strlen(eval_command);
    argv[argv_len_index] = new char[argv_len[argv_len_index]+1];
    strncpy(argv[argv_len_index], eval_command, argv_len[argv_len_index]+1);
    ++argv_len_index;

    // script/sha1
    argv_len[argv_len_index] = lua_script_or_sha1.size();
    argv[argv_len_index] = new char[argv_len[argv_len_index]+1];
    strncpy(argv[argv_len_index], lua_script_or_sha1.c_str(), argv_len[argv_len_index]+1);
    ++argv_len_index;

    // numkeys
    argv_len[argv_len_index] = 1;
    argv[argv_len_index] = new char[argv_len[argv_len_index]+1];
    strncpy(argv[argv_len_index], "1", argv_len[argv_len_index]+1);
    ++argv_len_index;

    // key
    argv_len[argv_len_index] = key.size();
    argv[argv_len_index] = new char[argv_len[argv_len_index]+1];
    strncpy(argv[argv_len_index], key.c_str(), argv_len[argv_len_index]+1);
    ++argv_len_index;

    for (std::vector<std::string>::size_type i=0; i<parameters.size(); ++i)
    {
        argv_len[argv_len_index] = parameters[i].size();
        argv[argv_len_index] = new char[argv_len[argv_len_index]+1];
        strncpy(argv[argv_len_index], parameters[i].c_str(), argv_len[argv_len_index]+1);
        ++argv_len_index;
    }

    FreeArgvHelper fah(argc, argv, argv_len);
    const std::string command_string;
    const redisReply* redis_reply = redis_command(excepted_reply_type, which, &key, eval_command, command_string, argc, (const char**)argv, argv_len);
    return RedisReplyHelper(redis_reply);
}

int CRedisClient::ttl(const std::string& key, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("TTL", sizeof("TTL")-1, &key, which);
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

void CRedisClient::set(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("SET", sizeof("SET")-1, &key, which);
    param_info.str1 = &value;
    (void)redis_command(REDIS_REPLY_STATUS, &param_info);
}

bool CRedisClient::setnx(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("SETNX", sizeof("SETNX")-1, &key, which);
    param_info.str1 = &value;
    return redis_command(REDIS_REPLY_INTEGER, &param_info) > 0;
}

void CRedisClient::setex(const std::string& key, const std::string& value, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(expired_seconds);
    struct ParamInfo param_info("SETEX", sizeof("SETEX")-1, &key, which);
    param_info.str1 = &str1;
    param_info.str2 = &value;
    (void)redis_command(REDIS_REPLY_STATUS, &param_info);
}

bool CRedisClient::setnxex(const std::string& key, const std::string& value, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str2 = "NX";
    const std::string str3 = "EX";
    const std::string str4 = any2string(expired_seconds);
    struct ParamInfo param_info("SET", sizeof("SET")-1, &key, which);
    param_info.str1 = &value;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.str4 = &str4;

    return redis_command(REDIS_REPLY_STATUS, &param_info) > 0;
}

bool CRedisClient::get(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("GET", sizeof("GET")-1, &key, which);
    param_info.value = value;
    redis_command(REDIS_REPLY_STRING, &param_info);
    return !value->empty();
}

int64_t CRedisClient::mget(const std::vector<std::string>& keys, std::vector<std::string>* values,
                           std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("MGET", sizeof("MGET")-1, NULL, which);
    param_info.array = &keys;
    param_info.values = values;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

bool CRedisClient::del(const std::string& key, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("DEL", sizeof("DEL")-1, &key, which);
    return redis_command(REDIS_REPLY_INTEGER, &param_info) > 0;
}

int64_t CRedisClient::incrby(const std::string& key, int64_t increment, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str6 = any2string(increment);
    struct ParamInfo param_info("INCRBY", sizeof("INCRBY")-1, &key, which);
    param_info.str6 = &str6;
    return redis_command(REDIS_REPLY_INTEGER, &param_info);
}

int64_t CRedisClient::incrby(const std::string& key, int64_t increment, int64_t expired_increment, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string lua_scripts = format_string("local n; n=redis.call('incrby','%s','%" PRId64"'); if (n==%" PRId64") then redis.call('expire', '%s', '%u') end; return n;", key.c_str(), increment, expired_increment, key.c_str(), expired_seconds);
    const RedisReplyHelper redis_reply = eval(key, lua_scripts, which);
    if (redis_reply->type != REDIS_REPLY_INTEGER)
    {
        THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(redis_reply->type, "unexpected type", which->first, which->second, "INCRBYEX", NULL);
    }

    int64_t ret = static_cast<int64_t>(redis_reply->integer);
    return ret;
}

int64_t CRedisClient::scan(int64_t cursor, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    struct ParamInfo param_info("SCAN", sizeof("SCAN")-1, NULL, which);

    param_info.str1 = &str1;
    param_info.values = values;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

int64_t CRedisClient::scan(int64_t cursor, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    const std::string str2 = "COUNT";
    const std::string str3 = any2string(count);
    struct ParamInfo param_info("SCAN", sizeof("SCAN")-1, NULL, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.values = values;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

int64_t CRedisClient::scan(int64_t cursor, const std::string& pattern, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    const std::string str2 = "MATCH";
    const std::string str3 = pattern;
    struct ParamInfo param_info("SCAN", sizeof("SCAN")-1, NULL, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.values = values;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

int64_t CRedisClient::scan(int64_t cursor, const std::string& pattern, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    const std::string str2 = "MATCH";
    const std::string str3 = pattern;
    const std::string str4 = "COUNT";
    const std::string str5 = any2string(count);
    struct ParamInfo param_info("SCAN", sizeof("SCAN")-1, NULL, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.str4 = &str4;
    param_info.str5 = &str5;
    param_info.values = values;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

////////////////////////////////////////////////////////////////////////////////
// LIST
int CRedisClient::llen(const std::string& key, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("LLEN", sizeof("LLEN")-1, &key, which);
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

bool CRedisClient::lpop(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("LPOP", sizeof("LPOP")-1, &key, which);
    param_info.value = value;
    (void)redis_command(REDIS_REPLY_STRING, &param_info);
    return !value->empty();
}

int CRedisClient::lpush(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    std::vector<std::string> values(1, value);
    return lpush(key, values, which);
}

int CRedisClient::lpush(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("LPUSH", sizeof("LPUSH")-1, &key, which);
    param_info.array = &values;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

int CRedisClient::lrange(const std::string& key, int64_t start, int64_t end, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str6 = any2string(start);
    const std::string str7 = any2string(end);
    struct ParamInfo param_info("LRANGE", sizeof("LRANGE")-1, &key, which);

    param_info.str6 = &str6;
    param_info.str7 = &str7;
    param_info.values = values;
    (void)redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(values->size());
}

bool CRedisClient::ltrim(const std::string& key, int64_t start, int64_t end, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str6 = any2string(start);
    const std::string str7 = any2string(end);
    struct ParamInfo param_info("LTRIM", sizeof("LTRIM")-1, &key, which);

    param_info.str6 = &str6;
    param_info.str7 = &str7;
    int64_t result = redis_command(REDIS_REPLY_STATUS, &param_info);
    return 1 == result;
}

bool CRedisClient::rpop(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("RPOP", sizeof("RPOP")-1, &key, which);
    param_info.value = value;
    int64_t result = redis_command(REDIS_REPLY_STRING, &param_info);
    return 1 == result;
}

int CRedisClient::rpush(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    std::vector<std::string> values(1, value);
    return rpush(key, values, which);
}

int CRedisClient::rpush(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("RPUSH", sizeof("RPUSH")-1, &key, which);
    param_info.array = &values;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

int CRedisClient::rpushx(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("RPUSHX", sizeof("RPUSHX")-1, &key, which);
    param_info.str1 = &value;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

////////////////////////////////////////////////////////////////////////////////
// HASH
bool CRedisClient::hdel(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    std::vector<std::string> fields(1);
    fields[0] = field;
    return hmdel(key, fields, which) > 0;
}

int CRedisClient::hmdel(const std::string& key, const std::vector<std::string>& fields, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("HDEL", sizeof("HDEL")-1, &key, which);
    param_info.array = &fields;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

bool CRedisClient::hexists(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("HEXISTS", sizeof("HEXISTS")-1, &key, which);
    param_info.str1 = &field;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return result > 0;
}

int CRedisClient::hlen(const std::string& key, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("HLEN", sizeof("HLEN")-1, &key, which);
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

bool CRedisClient::hset(const std::string& key, const std::string& field, const std::string& value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("HSET", sizeof("HSET")-1, &key, which);
    param_info.str1 = &field;
    param_info.str2 = &value;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return result > 0;
}

bool CRedisClient::hsetex(const std::string& key, const std::string& field, const std::string& value, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string lua_scripts = format_string("local n; n=redis.call('hset','%s','%s','%s'); if (n>0) then redis.call('expire', '%s', '%u') end; return n;", key.c_str(), field.c_str(), value.c_str(), key.c_str(), expired_seconds);
    const RedisReplyHelper redis_reply = eval(key, lua_scripts, which);

    if (redis_reply->type != REDIS_REPLY_INTEGER)
    {
        THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(redis_reply->type, "unexpected type", which->first, which->second, "INCRBY", NULL);
    }

    int ret = static_cast<int>(redis_reply->integer);
    return ret > 0;
}

bool CRedisClient::hsetnx(const std::string& key, const std::string& field, const std::string& value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("HSETNX", sizeof("HSETNX")-1, &key, which);
    param_info.str1 = &field;
    param_info.str2 = &value;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return result > 0;
}

bool CRedisClient::hsetnxex(const std::string& key, const std::string& field, const std::string& value, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string lua_scripts = format_string("local n; n=redis.call('hsetnx','%s','%s','%s'); if (n>0) then redis.call('expire', '%s', '%u') end; return n;", key.c_str(), field.c_str(), value.c_str(), key.c_str(), expired_seconds);
    const RedisReplyHelper redis_reply = eval(key, lua_scripts, which);

    if (redis_reply->type != REDIS_REPLY_INTEGER)
    {
        THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(redis_reply->type, "unexpected type", which->first, which->second, "HSETNXEX", NULL);
    }

    int ret = static_cast<int>(redis_reply->integer);
    return ret > 0;
}

bool CRedisClient::hget(const std::string& key, const std::string& field, std::string* value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("HGET", sizeof("HGET")-1, &key, which);
    param_info.str1 = &field;
    param_info.value = value;
    int64_t result = redis_command(REDIS_REPLY_STRING, &param_info);
    return 1 == result;
}

int64_t CRedisClient::hincrby(const std::string& key, const std::string& field, int64_t increment, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str6 = any2string(increment);
    struct ParamInfo param_info("HINCRBY", sizeof("HINCRBY")-1, &key, which);
    param_info.str1 = &field;
    param_info.str6 = &str6;
    return redis_command(REDIS_REPLY_INTEGER, &param_info);
}

void CRedisClient::hmincrby(const std::string& key, const std::vector<std::pair<std::string, int64_t> >& increments, std::vector<int64_t>* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string lua_scripts = format_string("local j=1;local results={};for i=1,#ARGV,2 do local f=ARGV[i];local v=ARGV[i+1];results[j]=redis.call('hincrby','%s',f,v);j=j+1; end;return results;", key.c_str());

    std::vector<std::string> parameters(2*increments.size());
    for (std::vector<std::pair<std::string, int64_t> >::size_type i=0,j=0; i<increments.size(); ++i,j+=2)
    {
        parameters[j] = increments[i].first;
        parameters[j+1] = any2string(increments[i].second);
    }

    (*g_debug_log)("[%s:%d]lua scripts: \n%s\n", __FILE__, __LINE__, lua_scripts.c_str());
    const RedisReplyHelper redis_reply = eval(key, lua_scripts, parameters, which);
    if (redis_reply->type != REDIS_REPLY_ARRAY)
    {
        int type = redis_reply->type;
        THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(type, "unexpected type", which->first, which->second, "INCRBY", NULL);
    }
    else if (values != NULL)
    {
        values->resize(redis_reply->elements);
        for (size_t i=0; i<redis_reply->elements; ++i)
            (*values)[i] = static_cast<int64_t>(redis_reply->element[i]->integer);
    }
}

void CRedisClient::hmset(const std::string& key, const std::map<std::string, std::string>& map, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("HMSET", sizeof("HMSET")-1, &key, which);
    param_info.in_map1 = &map;
    (void)redis_command(REDIS_REPLY_STATUS, &param_info);
}

int CRedisClient::hmget(const std::string& key, const std::vector<std::string>& fields, std::map<std::string, std::string>* map, bool keep_null, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("HMGET", sizeof("HMGET")-1, &key, which);
    param_info.array = &fields;
    param_info.out_map = map;
    param_info.keep_null = &keep_null;
    int64_t result = redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(result);
}

int CRedisClient::hgetall(const std::string& key, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("HGETALL", sizeof("HGETALL")-1, &key, which);
    param_info.out_map = map;
    (void)redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(map->size());
}

int CRedisClient::hstrlen(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("HSTRLEN", sizeof("HSTRLEN")-1, &key, which);
    param_info.str1 = &field;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

int CRedisClient::hkeys(const std::string& key, std::vector<std::string>* fields, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("HKEYS", sizeof("HKEYS")-1, &key, which);
    param_info.values = fields;
    (void)redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(fields->size());
}

int CRedisClient::hvals(const std::string& key, std::vector<std::string>* vals, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("HVALS", sizeof("HVALS")-1, &key, which);
    param_info.values = vals;
    (void)redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(vals->size());
}

int64_t CRedisClient::hscan(const std::string& key, int64_t cursor, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    struct ParamInfo param_info("HSCAN", sizeof("HSCAN")-1, &key, which);
    param_info.str1 = &str1;
    param_info.out_map = map;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

int64_t CRedisClient::hscan(const std::string& key, int64_t cursor, int count, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    const std::string str2 = "COUNT";
    const std::string str3 = any2string(count);
    struct ParamInfo param_info("HSCAN", sizeof("HSCAN")-1, &key, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.out_map = map;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

int64_t CRedisClient::hscan(const std::string& key, int64_t cursor, const std::string& pattern, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    const std::string str2 = "MATCH";
    const std::string str3 = pattern;
    struct ParamInfo param_info("HSCAN", sizeof("HSCAN")-1, &key, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.out_map = map;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

int64_t CRedisClient::hscan(const std::string& key, int64_t cursor, const std::string& pattern, int count, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    const std::string str2 = "MATCH";
    const std::string str3 = pattern;
    const std::string str4 = "COUNT";
    const std::string str5 = any2string(count);
    struct ParamInfo param_info("HSCAN", sizeof("HSCAN")-1, &key, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.str4 = &str4;
    param_info.str5 = &str5;
    param_info.out_map = map;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

////////////////////////////////////////////////////////////////////////////////
// SET
int CRedisClient::sadd(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("SADD", sizeof("SADD")-1, &key, which);
    param_info.str1 = &value;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

int CRedisClient::sadd(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("SADD", sizeof("SADD")-1, &key, which);
    param_info.array = &values;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

int CRedisClient::scard(const std::string& key, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("SCARD", sizeof("SCARD")-1, &key, which);
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

bool CRedisClient::sismember(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("SISMEMBER", sizeof("SISMEMBER")-1, &key, which);
    param_info.str1 = &value;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return 1 == result;
}

int CRedisClient::smembers(const std::string& key, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("SMEMBERS", sizeof("SMEMBERS")-1, &key, which);
    param_info.values = values;
    (void)redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(values->size());
}

bool CRedisClient::spop(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("SPOP", sizeof("SPOP")-1, &key, which);
    param_info.value = value;
    int64_t result = redis_command(REDIS_REPLY_STRING, &param_info);
    return 1 == result;
}

int CRedisClient::spop(const std::string& key, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(count);
    struct ParamInfo param_info("SPOP", sizeof("SPOP")-1, &key, which);

    param_info.str1 = &str1;
    param_info.values = values;
    int64_t result = redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(result);
}

int CRedisClient::srandmember(const std::string& key, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    std::string str1 = any2string(count);
    struct ParamInfo param_info("SRANDMEMBER", sizeof("SRANDMEMBER")-1, &key, which);

    param_info.str1 = &str1;
    param_info.values = values;
    (void)redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(values->size());
}

int CRedisClient::srem(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    std::vector<std::string> values(1);
    values[0] = value;
    return srem(key, values, which);
}

int CRedisClient::srem(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("SREM", sizeof("SREM")-1, &key, which);
    param_info.array = &values;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

int64_t CRedisClient::sscan(const std::string& key, int64_t cursor, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    struct ParamInfo param_info("SSCAN", sizeof("SSCAN")-1, &key, which);

    param_info.str1 = &str1;
    param_info.values = values;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

int64_t CRedisClient::sscan(const std::string& key, int64_t cursor, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    const std::string str2 = "COUNT";
    const std::string str3 = any2string(count);
    struct ParamInfo param_info("SSCAN", sizeof("SSCAN")-1, &key, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.values = values;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

int64_t CRedisClient::sscan(const std::string& key, int64_t cursor, const std::string& pattern, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    const std::string str2 = "MATCH";
    const std::string str3 = pattern;
    struct ParamInfo param_info("SSCAN", sizeof("SSCAN")-1, &key, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.values = values;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

int64_t CRedisClient::sscan(const std::string& key, int64_t cursor, const std::string& pattern, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    const std::string str2 = "MATCH";
    const std::string str3 = pattern;
    const std::string str4 = "COUNT";
    const std::string str5 = any2string(count);
    struct ParamInfo param_info("SSCAN", sizeof("SSCAN")-1, &key, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.str4 = &str4;
    param_info.str5 = &str5;
    param_info.values = values;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

////////////////////////////////////////////////////////////////////////////////
// SORTED SET
int CRedisClient::zrem(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("ZREM", sizeof("ZREM")-1, &key, which);
    param_info.str1 = &field;
    return static_cast<int>(redis_command(REDIS_REPLY_INTEGER, &param_info));
}

int CRedisClient::zrem(const std::string& key, const std::vector<std::string>& fields, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("ZREM", sizeof("ZREM")-1, &key, which);
    param_info.array = &fields;
    return static_cast<int>(redis_command(REDIS_REPLY_INTEGER, &param_info));
}

int CRedisClient::zadd(const std::string& key, const std::string& field, int64_t score, ZADDFLAG flag, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    std::map<std::string, int64_t> map;
    map[field] = score;
    return zadd(key, map, flag, which);
}

int CRedisClient::zadd(const std::string& key, const std::map<std::string, int64_t>& map, ZADDFLAG flag, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    std::string str1;
    if (Z_XX == flag)
        str1 = "XX";
    else if (Z_NX == flag)
        str1 = "NX";
    else if (Z_CH == flag)
        str1 = "CH";
    else if (flag != Z_NS)
        THROW_REDIS_EXCEPTION(ERR_PARAMETER, "invalid zadd flag");

    struct ParamInfo param_info("ZADD", sizeof("ZADD")-1, &key, which);
    if (!str1.empty())
        param_info.str1 = & str1;
    param_info.in_map2 = &map;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

int64_t CRedisClient::zcard(const std::string& key, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("ZCARD", sizeof("ZCARD")-1, &key, which);
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return result;
}

int64_t CRedisClient::zcount(const std::string& key, int64_t min, int64_t max , std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str6 = any2string(min);
    const std::string str7 = any2string(max);
    struct ParamInfo param_info("ZCOUNT", sizeof("ZCOUNT")-1, &key, which);

    param_info.str6 = &str6;
    param_info.str7 = &str7;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return result;
}


int64_t CRedisClient::zincrby(const std::string& key, const std::string& field, int64_t increment, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    std::string value;
    std::string str1 = any2string(increment);
    struct ParamInfo param_info("ZINCRBY", sizeof("ZINCRBY")-1, &key, which);

    param_info.str1 = &str1;
    param_info.str2 = &field;
    param_info.value = &value;
    (void)redis_command(REDIS_REPLY_STRING, &param_info);
    return atoll(value.c_str());
}

int CRedisClient::zrange(const std::string& key, int64_t start, int64_t end, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str6 = any2string(start);
    const std::string str7 = any2string(end);
    struct ParamInfo param_info("ZRANGE", sizeof("ZRANGE")-1, &key, which);

    param_info.str6 = &str6;
    param_info.str7 = &str7;
    param_info.tag = "withscores";
    param_info.tag_length = sizeof("withscores") - 1 ;
    param_info.withscores = &withscores;
    param_info.out_vec = vec;
    int64_t result = redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(result);
}

int CRedisClient::zrevrange(const std::string& key, int64_t start, int64_t end, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str6 = any2string(start);
    const std::string str7 = any2string(end);
    struct ParamInfo param_info("ZREVRANGE", sizeof("ZREVRANGE")-1, &key, which);

    param_info.str6 = &str6;
    param_info.str7 = &str7;
    param_info.tag = "withscores";
    param_info.tag_length = sizeof("withscores") - 1 ;
    param_info.withscores = &withscores;
    param_info.out_vec = vec;
    int64_t result = redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(result);
}

int CRedisClient::zrangebyscore(const std::string& key, int64_t min, int64_t max, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str6 = any2string(min);
    const std::string str7 = any2string(max);
    struct ParamInfo param_info("ZRANGEBYSCORE", sizeof("ZRANGEBYSCORE")-1, &key, which);

    param_info.str6 = &str6;
    param_info.str7 = &str7;
    param_info.tag = "withscores";
    param_info.tag_length = sizeof("withscores") - 1 ;
    param_info.withscores = &withscores;
    param_info.out_vec = vec;
    int64_t result = redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(result);
}

int CRedisClient::zrevrangebyscore(const std::string& key, int64_t max, int64_t min, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str6 = any2string(min);
    const std::string str7 = any2string(max);
    struct ParamInfo param_info("ZREVRANGEBYSCORE", sizeof("ZREVRANGEBYSCORE")-1, &key, which);

    param_info.str6 = &str6;
    param_info.str7 = &str7;
    param_info.tag = "withscores";
    param_info.tag_length = sizeof("withscores") - 1 ;
    param_info.withscores = &withscores;
    param_info.out_vec = vec;
    int64_t result = redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(result);
}

// ZRANGE key start stop [WITHSCORES]
// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
int CRedisClient::zrangebyscore(const std::string& key, int64_t min, int64_t max, int64_t offset, int64_t count, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str2 = any2string(min);
    const std::string str1 = any2string(max);
    const std::string str3 = "WITHSCORES";
    const std::string str4 = "LIMIT";
    const std::string str5 = any2string(offset);
    const std::string str6 = any2string(count);
    struct ParamInfo param_info("ZRANGEBYSCORE", sizeof("ZRANGEBYSCORE")-1, &key, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.str4 = &str4;
    param_info.str5 = &str5;
    param_info.str6 = &str6;
    param_info.withscores = &withscores;
    param_info.out_vec = vec;
    int64_t result = redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(result);
}

int CRedisClient::zrevrangebyscore(const std::string& key, int64_t min, int64_t max, int64_t offset, int64_t count, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str2 = any2string(min);
    const std::string str1 = any2string(max);
    const std::string str3 = "WITHSCORES";
    const std::string str4 = "LIMIT";
    const std::string str5 = any2string(offset);
    const std::string str6 = any2string(count);
    struct ParamInfo param_info("ZREVRANGEBYSCORE", sizeof("ZREVRANGEBYSCORE")-1, &key, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.str4 = &str4;
    param_info.str5 = &str5;
    param_info.str6 = &str6;
    param_info.withscores = &withscores;
    param_info.out_vec = vec;
    int64_t result = redis_command(REDIS_REPLY_ARRAY, &param_info);
    return static_cast<int>(result);
}

int CRedisClient::zrank(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("ZRANK", sizeof("ZRANK")-1, &key, which);
    param_info.str1 = &field;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

int CRedisClient::zrevrank(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    struct ParamInfo param_info("ZREVRANK", sizeof("ZREVRANK")-1, &key, which);
    param_info.str1 = &field;
    int64_t result = redis_command(REDIS_REPLY_INTEGER, &param_info);
    return static_cast<int>(result);
}

int64_t CRedisClient::zscore(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    std::string value;
    struct ParamInfo param_info("ZSCORE", sizeof("ZSCORE")-1, &key, which);

    param_info.str1 = &field;
    param_info.value = &value;
    (void)redis_command(REDIS_REPLY_STRING, &param_info);
    return atoll(value.c_str());
}

int64_t CRedisClient::zscan(const std::string& key, int64_t cursor, std::vector<std::pair<std::string, int64_t> >* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    struct ParamInfo param_info("ZSCAN", sizeof("ZSCAN")-1, &key, which);

    param_info.str1 = &str1;
    param_info.out_vec = values;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

int64_t CRedisClient::zscan(const std::string& key, int64_t cursor, int count, std::vector<std::pair<std::string, int64_t> >* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    const std::string str2 = "COUNT";
    const std::string str3 = any2string(count);
    struct ParamInfo param_info("ZSCAN", sizeof("ZSCAN")-1, &key, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.out_vec = values;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

int64_t CRedisClient::zscan(const std::string& key, int64_t cursor, const std::string& pattern, std::vector<std::pair<std::string, int64_t> >* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    const std::string str2 = "MATCH";
    const std::string str3 = pattern;
    struct ParamInfo param_info("ZSCAN", sizeof("ZSCAN")-1, &key, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.out_vec = values;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}

int64_t CRedisClient::zscan(const std::string& key, int64_t cursor, const std::string& pattern, int count, std::vector<std::pair<std::string, int64_t> >* values, std::pair<std::string, uint16_t>* which) throw (CRedisException)
{
    const std::string str1 = any2string(cursor);
    const std::string str2 = "MATCH";
    const std::string str3 = pattern;
    const std::string str4 = "COUNT";
    const std::string str5 = any2string(count);
    struct ParamInfo param_info("ZSCAN", sizeof("ZSCAN")-1, &key, which);

    param_info.str1 = &str1;
    param_info.str2 = &str2;
    param_info.str3 = &str3;
    param_info.str4 = &str4;
    param_info.str5 = &str5;
    param_info.out_vec = values;
    return redis_command(REDIS_REPLY_ARRAY, &param_info);
}


////////////////////////////////////////////////////////////////////////////////
// RAW COMMAND
const redisReply* CRedisClient::redis_command(int excepted_reply_type, std::pair<std::string, uint16_t>* which, const std::string* key, const char* command, const std::string& command_string) throw (CRedisException)
{
    return redis_command(excepted_reply_type, which, key, command, command_string, 0, NULL, NULL);
}

const redisReply* CRedisClient::redis_command(int excepted_reply_type, std::pair<std::string, uint16_t>* which, const std::string* key, const char* command, int argc, const char* argv[], const size_t* argv_len) throw (CRedisException)
{
    const std::string command_string;
    return redis_command(excepted_reply_type, which, key, command, command_string, argc, argv, argv_len);
}

const redisReply* CRedisClient::redis_command(int excepted_reply_type, std::pair<std::string, uint16_t>* which, const std::string* key, const char* command, const std::string& command_string, int argc, const char* argv[], const size_t* argv_len) throw (CRedisException)
{
    redisReply* redis_reply = NULL;
    int i = 0;
    int errcode = 0;
    std::string errmsg;
    std::pair<std::string, uint16_t> node;
    unsigned int slot = _cluster_mode? get_key_slot(key): 0;

    for (i=0; i<_retry_times; ++i)
    {
        errcode = 0;
        redisContext* redis_context = get_redis_context(slot, &node);
        if (NULL == redis_context)
        {
            errcode = ERR_INIT_REDIS_CONN;
            errmsg = "connect redis cluster failed";

            //THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(ERR_INIT_REDIS_CONN, "connect redis cluster failed", node.first, node.second, command, key.c_str());
            init();
            retry_sleep();
            continue;
        }

        if (which != NULL)
        {
            which->first = node.first;
            which->second = node.second;
        }
        if (redis_context->err != 0)
        {
            errcode = redis_context->err;
            errmsg = redis_context->errstr;
            (*g_error_log)("[%s:%d][%d/%d][%s][%s:%d](%d)%s\n", __FILE__, __LINE__, i, _retry_times, command, node.first.c_str(), node.second, errcode, errmsg.c_str());

            if (REDIS_ERR_IO == errcode)
            {
                init();
                retry_sleep();
                continue;
            }
            else
            {
                if (NULL == key)
                    THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(ERR_INIT_REDIS_CONN, errmsg, node.first, node.second, command, NULL);
                else
                    THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(ERR_INIT_REDIS_CONN, errmsg, node.first, node.second, command, key->c_str());
            }
        }

        if (0 == argc)
            redis_reply = (redisReply*)redisCommand(redis_context, command_string.c_str());
        else
            redis_reply = (redisReply*)redisCommandArgv(redis_context, argc, argv, argv_len);
        if (NULL == redis_reply)
        {
            // disconnnected
            errcode = ERROR_COMMAND;
            errmsg = format_string("redis `%s` error", command);
            (*g_error_log)("[%s:%d][%d/%d][%s][%s:%d](%d)%s|(%d, %d)%s\n", __FILE__, __LINE__, i, _retry_times, command, node.first.c_str(), node.second, errcode, errmsg.c_str(), errno, redis_context->err, redis_context->errstr);

            if ((EAGAIN == errno) || (EWOULDBLOCK == errno))
            {
                // Resource temporarily unavailable
                // 对于超时，不能重试，也许成功了，结果是不确定的！！！
                break;
            }
            else
            {
                init();
                retry_sleep();
                continue;
            }
        }
        else
        {
            errcode = 0;
            if (NULL == key)
                print_reply(command, NULL, slot, redis_reply, excepted_reply_type, node);
            else
                print_reply(command, key->c_str(), slot, redis_reply, excepted_reply_type, node);

            if (REDIS_REPLY_NIL == redis_reply->type)
            {
                break;
            }
            else if ((redis_reply->type != REDIS_REPLY_ERROR) && (-1 == excepted_reply_type))
            {
                break; // To support eval etc., a special type, can not decide type statically.
            }
            else if (redis_reply->type != excepted_reply_type)
            {
                //#define REDIS_REPLY_ERROR 6
                errcode = redis_reply->type;
                if (redis_reply->str != NULL)
                    errmsg = redis_reply->str;
                freeReplyObject(redis_reply);
                redis_reply = NULL;

                // CLUSTERDOWN The cluster is down (while master is down) （需重试错误）
                // WRONGTYPE Operation against a key holding the wrong kind of value （此种错误时不需重试）
                // LOADING Redis is loading the dataset in memory （需重试错误）
                // MOVED 6474 127.0.0.1:6380 （需重试错误）
                // ERR wrong number of arguments for 'sadd' command|(0)（此种错误时不需重试）
                // NOSCRIPT No matching script. Please use EVAL.
                (*g_error_log)("[%s:%d][%d/%d][%s][%s:%d](%d)%s|(%d)%s\n", __FILE__, __LINE__, i, _retry_times, command, node.first.c_str(), node.second, errcode, errmsg.c_str(), redis_context->err, redis_context->errstr);
                if ((0 == strncmp(errmsg.c_str(), "WRONGTYPE", sizeof("WRONGTYPE")-1)) ||
                    (0 == strncmp(errmsg.c_str(), "ERR", sizeof("ERR")-1)))
                {
                    break;
                }
                else if (0 == strncmp(errmsg.c_str(), "NOSCRIPT", sizeof("NOSCRIPT")-1))
                {
                    errcode = ERROR_NOSCRIPT;
                    break;
                }
                else
                {
                    init();
                    retry_sleep();
                    continue;
                }
            }

            break;
        }
    }

    if (errcode != 0)
    {
        if (redis_reply != NULL)
        {
            freeReplyObject(redis_reply);
            redis_reply = NULL;
        }

        //(*g_debug_log)("[%s:%d][%d/%d][%s][%s:%d](%d)%s\n", __FILE__, __LINE__, i, _retry_times, command, node.first.c_str(), node.second, errcode, errmsg.c_str());
        if (NULL == key)
            THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(errcode, errmsg, node.first, node.second, command, NULL);
        else
            THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(errcode, errmsg, node.first, node.second, command, key->c_str());
    }
    return redis_reply;
}

int64_t CRedisClient::redis_command(int excepted_reply_type, struct ParamInfo* param_info) throw (CRedisException)
{
    int64_t result = -1;
    size_t elements;
    size_t i;
    struct redisReply** element;

    int index = 0;
    int argc = calc_argc(param_info);
    char** argv = new char*[argc];
    size_t* argv_len = new size_t[argc];
    std::string str;

    // command
    argv_len[index] = param_info->command_length;
    argv[index] = new char[argv_len[index]];
    memcpy(argv[index], param_info->command, argv_len[index]);
    ++index;

    // key
    if (param_info->key != NULL)
    {
        argv_len[index] = (*param_info->key).size();
        argv[index] = new char[argv_len[index]];
        memcpy(argv[index], (*param_info->key).c_str(), argv_len[index]);
        ++index;
    }

    // str1
    if (param_info->str1 != NULL)
    {
        argv_len[index] = (*param_info->str1).size();
        argv[index] = new char[argv_len[index]];
        memcpy(argv[index], (*param_info->str1).c_str(), argv_len[index]);
        ++index;
    }

    // str2
    if (param_info->str2 != NULL)
    {
        argv_len[index] = (*param_info->str2).size();
        argv[index] = new char[argv_len[index]];
        memcpy(argv[index], (*param_info->str2).c_str(), argv_len[index]);
        ++index;
    }

    // str3
    if (param_info->str3 != NULL)
    {
        argv_len[index] = (*param_info->str3).size();
        argv[index] = new char[argv_len[index]];
        memcpy(argv[index], (*param_info->str3).c_str(), argv_len[index]);
        ++index;
    }

    // str4
    if (param_info->str4 != NULL)
    {
        argv_len[index] = (*param_info->str4).size();
        argv[index] = new char[argv_len[index]];
        memcpy(argv[index], (*param_info->str4).c_str(), argv_len[index]);
        ++index;
    }

    // str5
    if (param_info->str5 != NULL)
    {
        argv_len[index] = (*param_info->str5).size();
        argv[index] = new char[argv_len[index]];
        memcpy(argv[index], (*param_info->str5).c_str(), argv_len[index]);
        ++index;
    }

    // array
    if (param_info->array != NULL)
    {
        for (i=0; i<param_info->array->size(); ++i)
        {
            argv_len[index] = (*param_info->array)[i].size();
            argv[index] = new char[argv_len[index]];
            memcpy(argv[index], (*param_info->array)[i].c_str(), argv_len[index]);
            ++index;
        }
    }

    // in_map1
    if (param_info->in_map1 != NULL)
    {
        for (std::map<std::string, std::string>::const_iterator c_iter=(*param_info->in_map1).begin(); c_iter!=(*param_info->in_map1).end(); ++c_iter)
        {
            argv_len[index] = c_iter->first.size();
            argv[index] = new char[argv_len[index]];
            memcpy(argv[index], c_iter->first.c_str(), argv_len[index]);
            ++index;

            argv_len[index] = c_iter->second.size();
            argv[index] = new char[argv_len[index]];
            memcpy(argv[index], c_iter->second.c_str(), argv_len[index]);
            ++index;
        }
    }

    // in_map2
    if (param_info->in_map2 != NULL)
    {
        for (std::map<std::string, int64_t>::const_iterator c_iter=(*param_info->in_map2).begin(); c_iter!=(*param_info->in_map2).end(); ++c_iter)
        {
            // score
            str = any2string(c_iter->second);
            argv_len[index] = str.size();
            argv[index] = new char[argv_len[index]];
            memcpy(argv[index], str.c_str(), argv_len[index]);
            ++index;

            // value
            argv_len[index] = c_iter->first.size();
            argv[index] = new char[argv_len[index]];
            memcpy(argv[index], c_iter->first.c_str(), argv_len[index]);
            ++index;
        }
    }

    // str6
    if (param_info->str6 != NULL)
    {
        argv_len[index] = param_info->str6->size();
        argv[index] = new char[argv_len[index]];
        memcpy(argv[index], param_info->str6->c_str(), argv_len[index]);
        ++index;
    }

    // str7
    if (param_info->str7)
    {
        argv_len[index] = param_info->str7->size();
        argv[index] = new char[argv_len[index]];
        memcpy(argv[index], param_info->str7->c_str(), argv_len[index]);
        ++index;
    }

    // tag
    if (param_info->tag != NULL)
    {
        argv_len[index] = param_info->tag_length;
        argv[index] = new char[argv_len[index]];
        memcpy(argv[index], param_info->tag, argv_len[index]);
        ++index;
    }

    // value
    if (param_info->value != NULL) param_info->value->clear();
    // values
    if (param_info->values != NULL) param_info->values->clear();
    // out_map
    if (param_info->out_map != NULL) param_info->out_map->clear();
    // out_vec
    if (param_info->out_vec != NULL) param_info->out_vec->clear();

    // ERR syntax error
    // ERR unknown command 'SSCA'

    const FreeArgvHelper fah(argc, argv, argv_len);
    const redisReply* redis_reply = redis_command(excepted_reply_type, param_info->which, param_info->key, param_info->command, argc, (const char**)argv, argv_len);
    if (redis_reply != NULL)
    {
        if (REDIS_REPLY_STATUS == redis_reply->type)
        {
            if (0 == strcmp(redis_reply->str, "OK"))
                result = 1;

            if (param_info->value != NULL)
                param_info->value->assign(redis_reply->str, redis_reply->len);
        }
        else if (REDIS_REPLY_INTEGER == redis_reply->type)
        {
            result = redis_reply->integer;
        }
        else if (REDIS_REPLY_STRING == redis_reply->type)
        {
            R3C_ASSERT(param_info->value != NULL);

            result = 1;
            param_info->value->assign(redis_reply->str, redis_reply->len);
        }
        else if (REDIS_REPLY_ARRAY == redis_reply->type)
        {
            if (param_info->values != NULL)
            {
                R3C_ASSERT(NULL == param_info->out_map);

                if ((2 == redis_reply->elements) && (REDIS_REPLY_ARRAY == redis_reply->element[1]->type))
                {
                    // scan, sscan
                    elements = redis_reply->element[1]->elements;
                    element = redis_reply->element[1]->element;

                    if (REDIS_REPLY_STRING == redis_reply->element[0]->type)
                    {
                        // scan,sscan
                        //(gdb) p *redis_reply->element[0]
                        //$1 = {type = 1, integer = 0, len = 4, str = 0x657b00 "5632", elements = 0, element = 0x0}
                        result = static_cast<int64_t>(atoll(redis_reply->element[0]->str)); // cursor
                    }
                    else
                    {
                        result = 0;
                        (*g_debug_log)("[%s:%d] type=%d, command=%s\n", __FILE__, __LINE__, redis_reply->element[0]->type, param_info->command);
                    }
                }
                else
                {
                    elements = redis_reply->elements;
                    element = redis_reply->element;
                    result = static_cast<int64_t>(elements);
                }

                param_info->values->resize(elements);
                for (i=0; i<elements; ++i)
                {
                    const std::string v(element[i]->str, element[i]->len);
                    (*param_info->values)[i] = v;
                }
            }
            else if (param_info->out_vec != NULL) // zrange or zscan
            {
                if ((2 == redis_reply->elements) && (REDIS_REPLY_ARRAY == redis_reply->element[1]->type))
                {
                    // zscan
                    //(gdb) p *redis_reply->element[0]
                    //$1 = {type = 1, integer = 0, len = 5, str = 0x6584e0 "65536", elements = 0, element = 0x0}
                    result = static_cast<int64_t>(atoll(redis_reply->element[0]->str)); // cursor

                    for (i=0; i<redis_reply->element[1]->elements; i+=2)
                    {
                        const std::string k(redis_reply->element[1]->element[i]->str, redis_reply->element[1]->element[i]->len);
                        const std::string v(redis_reply->element[1]->element[i+1]->str, redis_reply->element[1]->element[i+1]->len);
                        param_info->out_vec->push_back(std::make_pair(k, atoll(v.c_str())));
                    }
                }
                else
                {
                    result = 0;
                    for (i=0; i<redis_reply->elements;)
                    {
                        ++result;

                        const std::string k(redis_reply->element[i]->str, redis_reply->element[i]->len);
                        if (!*param_info->withscores)
                        {
                            param_info->out_vec->push_back(std::make_pair(k, 0));
                            i += 2;
                        }
                        else
                        {
                            const std::string v(redis_reply->element[i+1]->str, redis_reply->element[i+1]->len);
                            param_info->out_vec->push_back(std::make_pair(k, atoll(v.c_str())));
                            i += 2;
                        }
                    }
                }
            } // zrange or zscan
            else if (param_info->out_map != NULL) // hmget & hgetall & hscan
            {
                if (NULL == param_info->keep_null) // hgetall & hscan
                {
                    if ((2 == redis_reply->elements) && (REDIS_REPLY_ARRAY == redis_reply->element[1]->type))
                    {
                        // hscan
                        //(gdb) p *redis_reply->element[0]
                        //$47 = {type = 1, integer = 0, len = 1, str = 0x6585a0 "6", elements = 0, element = 0x0}
                        result = static_cast<int64_t>(atoll(redis_reply->element[0]->str)); // cursor
                        elements = redis_reply->element[1]->elements;
                        element = redis_reply->element[1]->element;
                    }
                    else
                    {
                        elements = redis_reply->elements;
                        element = redis_reply->element;
                        result = static_cast<int64_t>(elements / 2);
                    }

                    for (i=0; i<elements; i+=2)
                    {
                        const std::string k(element[i]->str, element[i]->len);
                        const std::string v(element[i+1]->str, element[i+1]->len);
                        (*param_info->out_map)[k] = v;
                    }
                } // hgetall & hscan
                else // hmget
                {
                    R3C_ASSERT(param_info->array != NULL);

                    result = 0;
                    for (i=0; i<redis_reply->elements; ++i)
                    {
                        if (REDIS_REPLY_STRING == redis_reply->element[i]->type)
                        {
                            ++result;
                            const std::string value(redis_reply->element[i]->str, redis_reply->element[i]->len);
                            (*param_info->out_map)[(*param_info->array)[i]] = value;
                        }
                        else if (REDIS_REPLY_INTEGER == redis_reply->element[i]->type)
                        {
                            ++result;
                            (*param_info->out_map)[(*param_info->array)[i]] = any2string(redis_reply->element[i]->integer);
                        }
                        else if (REDIS_REPLY_NIL == redis_reply->element[i]->type)
                        {
                            if (*param_info->keep_null)
                                (*param_info->out_map)[(*param_info->array)[i]] = std::string("");
                        }
                    }
                } // hmget
            } // hmget & hgetall
        }

        freeReplyObject(const_cast<redisReply*>(redis_reply));
        redis_reply = NULL;
    }

    return result;
}

int CRedisClient::calc_argc(const struct ParamInfo* param_info) const
{
    size_t argc = 1;
    if (param_info->key != NULL) argc += 1;

    if (param_info->str1 != NULL) argc += 1;
    if (param_info->str2 != NULL) argc += 1;
    if (param_info->str3 != NULL) argc += 1;
    if (param_info->str4 != NULL) argc += 1;
    if (param_info->str5 != NULL) argc += 1;

    if (param_info->array != NULL) argc += param_info->array->size();
    if (param_info->in_map1 != NULL) argc += param_info->in_map1->size() + param_info->in_map1->size();
    if (param_info->in_map2 != NULL) argc += param_info->in_map2->size() + param_info->in_map2->size();

    if (param_info->str6 != NULL) argc += 1;
    if (param_info->str7 != NULL) argc += 1;

    if (param_info->tag != NULL) argc += 1;
    return static_cast<int>(argc);
}

////////////////////////////////////////////////////////////////////////////////
void CRedisClient::parse_nodes() throw (CRedisException)
{
    std::string::size_type len = 0;
    std::string::size_type pos = 0;
    std::string::size_type comma_pos = 0;

    _nodes.clear();
    while (comma_pos != std::string::npos)
    {
        comma_pos = _nodes_string.find(',', pos);
        if (comma_pos != std::string::npos)
            len = comma_pos - pos;
        else
            len = _nodes_string.size() - comma_pos;

        const std::string str = _nodes_string.substr(pos, len);
        std::string::size_type colon_pos = str.find(':');
        if (colon_pos == std::string::npos)
            THROW_REDIS_EXCEPTION(ERR_PARAMETER, "parameter[nodes] error");

        const std::string ip = str.substr(0, colon_pos);
        const std::string port = str.substr(colon_pos+1);
        _nodes.push_back(std::make_pair(ip, atoi(port.c_str())));

        // Next node
        pos = comma_pos + 1;
    }
}

void CRedisClient::init() throw (CRedisException)
{
    clear();

    if (_cluster_mode)
    {
        std::vector<struct NodeInfo> nodes_info;
        int num_nodes = list_nodes(&nodes_info);

        for (int i=0; i<num_nodes; ++i)
        {
            const struct NodeInfo& node_info = nodes_info[i];

            for (std::vector<std::pair<int, int> >::size_type j=0; j<node_info.slots.size(); ++j)
            {
                for (int slot=node_info.slots[j].first; slot<=node_info.slots[j].second; ++slot)
                {
                    struct SlotInfo* slot_info = new struct SlotInfo;
                    slot_info->slot = slot;
                    slot_info->redis_context = NULL;
                    slot_info->node.first = inet_addr(node_info.ip.c_str());
                    slot_info->node.second = node_info.port;

                    _slots[slot] = slot_info;
                }
            }
        }
    }
}

redisContext* CRedisClient::get_redis_context(unsigned int slot, std::pair<std::string, uint16_t>* node) throw (CRedisException)
{
    redisContext* redis_context = NULL;

    if (!_cluster_mode)
    {
        node->first = _nodes[0].first;
        node->second = _nodes[0].second;

        if (_redis_context != NULL)
        {
            redis_context = _redis_context;
        }
        else
        {
            if (_connect_timeout_milliseconds <= 0)
            {
                redis_context = redisConnect(node->first.c_str(), node->second);
				
            }
            else
            {
                struct timeval connect_timeout;
                connect_timeout.tv_sec = _connect_timeout_milliseconds / 1000;
                connect_timeout.tv_usec = (_connect_timeout_milliseconds % 1000) * 1000;
                redis_context = redisConnectWithTimeout(node->first.c_str(), node->second, connect_timeout);
            }

			

            _redis_context = redis_context;
            if (NULL == redis_context)
            {
                (*g_error_log)("[%s:%d][standalone]redisConnect failed\n", __FILE__, __LINE__, slot);
            }
            else if (_data_timeout_milliseconds > 0)
            {
				if(_password.size() > 0)
				{
					redisReply* redis_reply = (redisReply*)redisCommand(redis_context, "auth %s", _password.c_str());
					if (NULL == redis_reply)
					{
						(*g_error_log)("[%s:%d]auth failure\n", __FILE__, __LINE__);		
					}

				}
                struct timeval data_timeout;
                data_timeout.tv_sec = _data_timeout_milliseconds / 1000;
                data_timeout.tv_usec = (_data_timeout_milliseconds % 1000) * 1000;
                redisSetTimeout(redis_context, data_timeout);
            }
        }
    }
    else
    {
        R3C_ASSERT(slot < CLUSTER_SLOTS);

        if (slot < CLUSTER_SLOTS)
        {
            struct in_addr in;
            struct SlotInfo* slot_info = _slots[slot];
            if (NULL == slot_info)
            {
                // Maybe master fail
                (*g_error_log)("[%s:%d]slot[%u] not exists\n", __FILE__, __LINE__, slot);
            }
            else
            {
                in.s_addr = slot_info->node.first;
                node->first = inet_ntoa(in);
                node->second = slot_info->node.second;

                if (slot_info->redis_context != NULL)
                {
                    redis_context = slot_info->redis_context;
                }
                else
                {
                    std::map<std::pair<uint32_t, uint16_t>, redisContext*>::const_iterator iter = _redis_contexts.find(slot_info->node);
                    if (iter != _redis_contexts.end())
                    {
                        redis_context = iter->second;
                        slot_info->redis_context = redis_context;
                    }
                    else
                    {
                        if (_connect_timeout_milliseconds <= 0)
                        {
                            redis_context = redisConnect(node->first.c_str(), node->second);
							
                        }
                        else
                        {
                            struct timeval connect_timeout;
                            connect_timeout.tv_sec = _connect_timeout_milliseconds / 1000;
                            connect_timeout.tv_usec = (_connect_timeout_milliseconds % 1000) * 1000;
                            redis_context = redisConnectWithTimeout(node->first.c_str(), node->second, connect_timeout);
                        }

                        if (NULL == redis_context)
                        {
                            (*g_error_log)("[%s:%d]slot[%u] redisConnect failed\n", __FILE__, __LINE__, slot);
                        }
                        else
                        {                            
							if(_password.size() > 0)
							{
								redisReply* redis_reply = (redisReply*)redisCommand(redis_context, "auth %s", _password.c_str());
								if (NULL == redis_reply)
								{
									(*g_error_log)("[%s:%d]auth failure\n", __FILE__, __LINE__);		
								}

							}
                            slot_info->redis_context = redis_context;
                            _redis_contexts.insert(std::make_pair(slot_info->node, redis_context));

                            if (_data_timeout_milliseconds > 0)
                            {
                                struct timeval data_timeout;
                                data_timeout.tv_sec = _data_timeout_milliseconds / 1000;
                                data_timeout.tv_usec = (_data_timeout_milliseconds % 1000) * 1000;
                                redisSetTimeout(redis_context, data_timeout);
                            }
                        }
                    }
                } // if (slot_info->redis_context != NULL)
            } // if (NULL == slot_info)
        } // if (slot < CLUSTER_SLOTS)
    } // cluster_mode

    return redis_context;
}

void CRedisClient::choose_node(int seed_factor, std::pair<std::string, uint16_t>* node) const
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    srandom(tv.tv_usec + seed_factor);

    long index = random() % _nodes.size();
    node->first = _nodes[index].first;
    node->second = _nodes[index].second;

    //(*g_debug_log)("[%d][%s:%d]%s:%d chosen\n", seed_factor, __FILE__, __LINE__, node->first.c_str(), node->second);
}

redisContext* CRedisClient::connect_node(int* errcode, std::string* errmsg, std::pair<std::string, uint16_t>* node) const
{
    redisContext* redis_context = NULL;
    std::pair<std::string, uint16_t> old_node;

    for (int i=0; i<static_cast<int>(_nodes.size())+2; ++i)
    {
        choose_node(i, node);
        if ((node->first == old_node.first) && (node->second == old_node.second))
            continue;

        old_node.first = node->first;
        old_node.second = node->second;
        if (_connect_timeout_milliseconds <= 0)
        {
            redis_context = redisConnect(node->first.c_str(), node->second);
			
        }
        else
        {
            struct timeval connect_timeout;
            connect_timeout.tv_sec = _connect_timeout_milliseconds / 1000;
            connect_timeout.tv_usec = (_connect_timeout_milliseconds % 1000) * 1000;
            redis_context = redisConnectWithTimeout(node->first.c_str(), node->second, connect_timeout);
        }

        if (NULL == redis_context)
        {
            *errcode = errno;
            *errmsg = "can't allocate redis context";
            (*g_error_log)("[%s:%d][%d]allocate redis context failed: %s:%d\n", __FILE__, __LINE__, i, node->first.c_str(), node->second);
            continue;
        }
        else
        {
			if(_password.size() > 0)
			{
				redisReply* redis_reply = (redisReply*)redisCommand(redis_context, "auth %s", _password.c_str());
				if (NULL == redis_reply)
				{
					(*g_error_log)("[%s:%d]auth failure\n", __FILE__, __LINE__);		
				}

			}
            if (0 == redis_context->err)
            {
                if (_data_timeout_milliseconds <= 0)
                {
                    return redis_context;
                }
                else
                {
                    struct timeval data_timeout;
                    data_timeout.tv_sec = _data_timeout_milliseconds / 1000;
                    data_timeout.tv_usec = (_data_timeout_milliseconds % 1000) * 1000;

                    // 出错redisSetTimeout返回REDIS_ERR，
                    // 并设置redis_context->err为REDIS_ERR_IO，redis_context->errstr存储出错信息。
                    redisSetTimeout(redis_context, data_timeout);

                    // 不能通过redisSetTimeout的返回值来判断成功与否，
                    // 原因是如果是非阻塞的redis_context，总是返回REDIS_ERR，
                    // 而这个时候redis_context->err的值可能是0，这种情况不认为是出错，而应视作为成功。
                    if (0 == redis_context->err)
                    {
                        return redis_context;
                    }
                }
            }

            if (redis_context->err != 0)
            {
                // REDIS_ERR_IO should use the "errno" variable to find out what is wrong
                // For other values, the "errstr" field will hold a description.

                *errcode = redis_context->err;
                if (REDIS_ERR_IO == *errcode)
                    *errmsg = strerror(errno);
                else
                    *errmsg = redis_context->errstr;
                redisFree(redis_context);
                redis_context = NULL;

                (*g_error_log)("[%d:%d][%s:%d][%s:%d]redisConnect: (%d)%s\n", i, static_cast<int>(_nodes.size()), __FILE__, __LINE__, node->first.c_str(), node->second, *errcode, errmsg->c_str());
                if (*errcode != REDIS_ERR_IO)
                    break;
            }
        }
    }

    return NULL;
}

void CRedisClient::clear()
{
    if (_cluster_mode)
    {
        clear_redis_contexts();
        clear_slots();
    }
    else if (_redis_context != NULL)
    {
        redisFree(_redis_context);
        _redis_context = NULL;
    }
}

void CRedisClient::clear_redis_contexts()
{
    for (std::map<std::pair<uint32_t, uint16_t>, redisContext*>::iterator iter=_redis_contexts.begin(); iter!=_redis_contexts.end(); ++iter)
    {
        redisContext* redis_context = iter->second;
        redisFree(redis_context);
    }
    _redis_contexts.clear();
}

void CRedisClient::clear_slots()
{
    for (int i=0; i<CLUSTER_SLOTS; ++i)
    {
        struct SlotInfo* slot_info = _slots[i];
        delete slot_info;
        _slots[i] = NULL;
    }
}

void CRedisClient::retry_sleep() const
{
    if (_retry_sleep_milliseconds > 0)
        millisleep(_retry_sleep_milliseconds);
}

} // namespace r3c {
