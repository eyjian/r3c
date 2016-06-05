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
#ifndef REDIS_CLUSTER_CLIENT_H
#define REDIS_CLUSTER_CLIENT_H
#include <assert.h>
#include <exception>
#include <hiredis/hiredis.h>
#include <inttypes.h> // PRId64
#include <iostream>
#include <map>
#include <ostream>
#include <sstream>
#include <stdint.h> // uint16_t
#include <string>
#include <vector>
namespace r3c {

// Error code
enum
{
    ERR_PARAMETER = -1,       // Parameter error
    ERR_INIT_REDIS_CONN = -2, // Initialize redis connection error
    ERROR_COMMAND = -3,       // Command error
    ERROR_CONNECT_REDIS = -4, // Can not connect any cluster node
    ERROR_FORMAT = -5         // Format error
};

// Cluster node info
struct NodeInfo
{
    std::string id;         // The node ID, a 40 characters random string generated when a node is created and never changed again (unless CLUSTER RESET HARD is used)
    std::string ip;         // The node IP
    uint16_t port;          // The node port
    std::string flags;      // A list of comma separated flags: myself, master, slave, fail?, fail, handshake, noaddr, noflags
    bool is_master;         // true if node is master, false if node is salve
    std::string master_id;  // The replication master
    int ping_sent;          // Milliseconds unix time at which the currently active ping was sent, or zero if there are no pending pings
    int pong_recv;          // Milliseconds unix time the last pong was received
    int epoch;              // The configuration epoch (or version) of the current node (or of the current master if the node is a slave). Each time there is a failover, a new, unique, monotonically increasing configuration epoch is created. If multiple nodes claim to serve the same hash slots, the one with higher configuration epoch wins
    bool connected;         // The state of the link used for the node-to-node cluster bus
    std::vector<std::pair<int, int> > slots; // A hash slot number or range
};

// Set NULL to discard log
typedef void (*LOG_WRITE)(const char* format, ...);
void set_info_log_write(LOG_WRITE info_log);
void set_debug_log_write(LOG_WRITE debug_log);

int split(std::vector<std::string>* tokens, const std::string& source, const std::string& sep, bool skip_sep=false);
void millisleep(uint32_t millisecond);
unsigned int get_key_slot(const std::string& key);
std::string format_string(const char* format, ...) __attribute__((format(printf, 1, 2)));
std::ostream& operator <<(std::ostream& os, const struct NodeInfo& node_info);

template <typename T>
inline std::string any2string(T m)
{
    std::stringstream ss;
    ss << m;
    return ss.str();
}

////////////////////////////////////////////////////////////////////////////////
class CRedisException: public std::exception
{
public:
    CRedisException(int errcode, const std::string& errmsg, const char* file, int line, const std::string& node_ip=std::string("-"), uint16_t node_port=0, const char* command=NULL, const char* key=NULL) throw ();
    virtual ~CRedisException() throw () {}
    virtual const char* what() const throw ();
    std::string str() const throw ();

    const char* file() const throw () { return _file.c_str(); }
    int line() const throw () { return _line; }
    const char* node_ip() const throw () { return _node_ip.c_str(); }
    uint16_t node_port() const throw () { return _node_port; }
    const char* command() const throw() { return _command.c_str(); }
    const char* key() const throw() { return _key.c_str(); }

private:
    const int _errcode;
    const std::string _errmsg;
    const std::string _file;
    const int _line;
    const std::string _node_ip;
    const uint16_t _node_port;
    std::string _command;
    std::string _key;
};

////////////////////////////////////////////////////////////////////////////////
struct SlotInfo; // Forward declare

class CRedisClient
{
public:
    CRedisClient(const std::string& nodes, int timeout_milliseconds=1000) throw (CRedisException);
    ~CRedisClient();
    void set_retry(int retry_times, int retry_sleep_milliseconds);

public:
    int list_nodes(std::vector<struct NodeInfo>* nodes_info, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);

    // key value
    bool exists(const std::string& key, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    bool expire(const std::string& key, uint32_t seconds, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    void set(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    bool setnx(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    void setex(const std::string& key, const std::string& value, uint32_t seconds, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    bool get(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    bool del(const std::string& key, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int64_t incrby(const std::string& key, int64_t increment, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);

    // list
    int llen(const std::string& key, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    bool lpop(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int lpush(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int lpush(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int lrange(const std::string& key, int start, int end, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    bool ltrim(const std::string& key, int start, int end, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    bool rpop(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int rpush(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int rpush(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int rpushx(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);

    // hash
    bool hdel(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int hdel(const std::string& key, const std::vector<std::string>& fields, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    bool hexists(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int hlen(const std::string& key, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    bool hset(const std::string& key, const std::string& field, const std::string& value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    bool hsetnx(const std::string& key, const std::string& field, const std::string& value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    bool hget(const std::string& key, const std::string& field, std::string* value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int64_t hincrby(const std::string& key, const std::string& field, int64_t increment, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    void hmset(const std::string& key, const std::map<std::string, std::string>& map, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int hmget(const std::string& key, const std::vector<std::string>& fields, std::map<std::string, std::string>* map, bool keep_null=false, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int hgetall(const std::string& key, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int hstrlen(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int hkeys(const std::string& key, std::vector<std::string>* fields, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int hvals(const std::string& key, std::vector<std::string>* vals, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);

    // set
    int sadd(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int sadd(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int scard(const std::string& key, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    bool sismember(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int smembers(const std::string& key, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    bool spop(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int srandmember(const std::string& key, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int srem(const std::string& key,const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int sscan(const std::string& key, const std::string& pattern, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);

    // sort set
    int zadd(const std::string& key, const std::string& field, double score, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int zadd(const std::string& key, const std::map<std::string, double>& map, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int zcount(const std::string& key, double min, double max , std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    double zincrby(const std::string& key, const std::string& field, double increment, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int zrange(const std::string& key, int start, int end, bool withscores, std::map<std::string, double>* map, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int zrank(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int zrevrank(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    double zscore(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);

    // raw command, binary unsafe
    const redisReply* redis_command(int excepted_reply_type, std::pair<std::string, uint16_t>* which, const std::string& key, const char* command, const std::string& command_string) throw (CRedisException);
    // raw command, binary safe
    const redisReply* redis_command(int excepted_reply_type, std::pair<std::string, uint16_t>* which, const std::string& key, const char* command, int argc, const char* argv[], const size_t* argv_len) throw (CRedisException);

private:
    const redisReply* redis_command(int excepted_reply_type, std::pair<std::string, uint16_t>* which, const std::string& key, const char* command, const std::string& command_string, int argc, const char* argv[], const size_t* argv_len) throw (CRedisException);
    int64_t redis_command(int excepted_reply_type, const char* command, size_t command_length, const std::string* key, const std::string* str1, const std::string* str2, const std::vector<std::string>* array, const std::map<std::string, std::string>* in_map1, const std::map<std::string, double>* in_map2, const double* m1, const double* m2, const char* tag, size_t tag_length, std::string* value, std::vector<std::string>* values, std::map<std::string, std::string>* out_map1, std::map<std::string, double>* out_map2, const bool* keep_null, const bool* withscores, std::pair<std::string, uint16_t>* which=NULL) throw (CRedisException);
    int calc_argc(const char* command, const std::string* key, const std::string* str1, const std::string* str2, const std::vector<std::string>* array, const std::map<std::string, std::string>* in_map1, const std::map<std::string, double>* in_map2, const double* m1, const double* m2, const char* tag) const;

private:
    void parse_nodes() throw (CRedisException);
    void init() throw (CRedisException);
    redisContext* get_redis_context(unsigned int slot, std::pair<std::string, uint16_t>* node);
    void choose_node(int seed_factor, std::pair<std::string, uint16_t>* node) const;
    redisContext* connect_node(int* errcode, std::string* errmsg, std::pair<std::string, uint16_t>* node) const;

private:
    void clear();
    void clear_redis_contexts();
    void clear_slots();
    void retry_sleep() const;

private:
    std::string _nodes_string;
    int _timeout_milliseconds;
    int _retry_times;
    int _retry_sleep_milliseconds;
    std::map<std::pair<uint32_t, uint16_t>, redisContext*> _redis_contexts;
    std::vector<std::pair<std::string, uint16_t> > _nodes;
    std::vector<struct SlotInfo*> _slots;
};

} // namespace r3c {
#endif // REDIS_CLUSTER_CLIENT_H
