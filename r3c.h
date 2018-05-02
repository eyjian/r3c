#ifndef REDIS_CLUSTER_CLIENT_H
#define REDIS_CLUSTER_CLIENT_H
#include <assert.h>
#include <hiredis/hiredis.h>
#include <inttypes.h>
#include <map>
#include <ostream>
#include <set>
#include <sstream>
#include <stdint.h>
#include <string>
#include <vector>

#define R3C_ASSERT assert

#define PRINT_COLOR_NONE         "\033[m"
#define PRINT_COLOR_RED          "\033[0;32;31m"
#define PRINT_COLOR_YELLOW       "\033[1;33m"
#define PRINT_COLOR_BLUE         "\033[0;32;34m"
#define PRINT_COLOR_GREEN        "\033[0;32;32m"
#define PRINT_COLOR_WHITE        "\033[1;37m"
#define PRINT_COLOR_CYAN         "\033[0;36m"
#define PRINT_COLOR_PURPLE       "\033[0;35m"
#define PRINT_COLOR_BROWN        "\033[0;33m"
#define PRINT_COLOR_DARY_GRAY    "\033[1;30m"
#define PRINT_COLOR_LIGHT_RED    "\033[1;31m"
#define PRINT_COLOR_LIGHT_GREEN  "\033[1;32m"
#define PRINT_COLOR_LIGHT_BLUE   "\033[1;34m"
#define PRINT_COLOR_LIGHT_CYAN   "\033[1;36m"
#define PRINT_COLOR_LIGHT_PURPLE "\033[1;35m"
#define PRINT_COLOR_LIGHT_GRAY   "\033[0;37m"

namespace r3c {

extern void millisleep(int milliseconds);
extern std::string format_string(const char* format, ...);
extern int split(std::vector<std::string>* tokens, const std::string& source, const std::string& sep, bool skip_sep=false);
extern unsigned int get_key_slot(const std::string* key);

template <typename T>
inline std::string any2string(T m)
{
    std::stringstream ss;
    ss << m;
    return ss.str();
}

enum ReadPolicy
{
    RP_ONLEY_MASTER // Always read from master
};

// Consts
enum
{
    // CLUSTERDOWN need more than 6s
    RETRY_TIMES = 8,                     // Default value

    // Default value, sleep 1000ms to retry
    //
    // 如果指定了编译宏SLEEP_USE_POLL的值为1，则sleep基于poll实现，否则基于nanosleep实现
    // nanosleep为精准的sleep，而poll版本则可能被中断提前结束，
    // 但如果是在libco中，则建议使用poll版本，否则会挂起调用协程指定时长,
    // 原因是libco没有对nanosleep做HOOK处理，但HOOK了poll。
    //
    // 注意，默认没有定义宏SLEEP_USE_POLL，可手工修改Makefile添加定义。
    RETRY_SLEEP_MILLISECONDS = 1000,

    CONNECT_TIMEOUT_MILLISECONDS = 1000, // Connect timeout milliseconds
    DATA_TIMEOUT_MILLISECONDS = 1000     // Read and write socket timeout milliseconds
};

// Error code
enum
{
    ERROR_PARAMETER = -1,              // Parameter error
    ERROR_INIT_REDIS_CONN = -2,        // Initialize redis connection error
    ERROR_COMMAND = -3,                // Command error
    ERROR_CONNECT_REDIS = -4,          // Can not connect any cluster node
    ERROR_FORMAT = -5,                 // Format error
    ERROR_NOT_SUPPORT = -6,            // Not support
    ERROR_SLOT_NOT_EXIST = -7,         // Slot not exists
    ERROR_NOSCRIPT = -8,               // NOSCRIPT No matching script
    ERROR_UNKNOWN_REPLY_TYPE = -9,     // unknhown reply type
    ERROR_NIL = -10,                   // Redis return Nil
    ERROR_INVALID_COMMAND = -11,       // Invalid command
    ERROR_ZERO_KEY = -12,              // Key size is zero
    ERROR_REDIS_CONTEXT = -13,         // Can't allocate redis context
    ERROR_REDIS_AUTH = 14,             // Authorization failed
    ERROR_UNEXCEPTED_REPLY_TYPE = -15, // Unexcepted reply type
    ERROR_REPLY_FORMAT = -16           // Reply format error
};

enum ZADDFLAG
{
    Z_NS, // Don't set options
    Z_XX, // Only update elements that already exist. Never add elements.
    Z_NX, // Don't update already existing elements. Always add new elements.
    Z_CH  // Modify the return value from the number of new elements added
};
std::string zaddflag2str(ZADDFLAG zaddflag);

////////////////////////////////////////////////////////////////////////////////

// Set NULL to discard log
typedef void (*LOG_WRITE)(const char* format, ...);
void set_error_log_write(LOG_WRITE error_log);
void set_info_log_write(LOG_WRITE info_log);
void set_debug_log_write(LOG_WRITE debug_log);

struct NodeInfo
{
    std::string id;         // The node ID, a 40 characters random string generated when a node is created and never changed again (unless CLUSTER RESET HARD is used)
    std::string ip;         // The node IP
    uint16_t port;          // The node port
    std::string flags;      // A list of comma separated flags: myself, master, slave, fail?, fail, handshake, noaddr, noflags
    std::string master_id;  // The replication master
    int ping_sent;          // Milliseconds unix time at which the currently active ping was sent, or zero if there are no pending pings
    int pong_recv;          // Milliseconds unix time the last pong was received
    int epoch;              // The configuration epoch (or version) of the current node (or of the current master if the node is a slave). Each time there is a failover, a new, unique, monotonically increasing configuration epoch is created. If multiple nodes claim to serve the same hash slots, the one with higher configuration epoch wins
    bool connected;         // The state of the link used for the node-to-node cluster bus
    std::vector<std::pair<int, int> > slots; // A hash slot number or range

    std::string str() const;
    bool is_master() const;
    bool is_slave() const;
    bool is_fail() const;
};
std::ostream& operator <<(std::ostream& os, const struct NodeInfo& node_info);

struct ClusterInfo
{
    int cluster_state;
    int cluster_slots_assigned;
    int cluster_slots_ok;
    int cluster_slots_pfail;
    int cluster_slots_fail;
    int cluster_known_nodes;
    int cluster_size;
    int cluster_current_epoch;
    int cluster_my_epoch;
    int64_t cluster_stats_messages_sent;
    int64_t cluster_stats_messages_received;
};

// Help to free redisReply automatically
// DO NOT use RedisReplyHelper for nested redisReply
class RedisReplyHelper
{
public:
    RedisReplyHelper()
        : _redis_reply(NULL)
    {
    }

    RedisReplyHelper(const redisReply* redis_reply)
        : _redis_reply(redis_reply)
    {
    }

    RedisReplyHelper(const RedisReplyHelper& other)
    {
        _redis_reply = other.detach();
    }

    ~RedisReplyHelper()
    {
        if (_redis_reply != NULL)
        {
            freeReplyObject((void*)_redis_reply);
        }
    }

    operator bool() const
    {
        return _redis_reply != NULL;
    }

    void free()
    {
        if (_redis_reply != NULL)
        {
            freeReplyObject((void*)_redis_reply);
            _redis_reply = NULL;
        }
    }

    const redisReply* get() const
    {
        return _redis_reply;
    }

    const redisReply* detach() const
    {
        const redisReply* redis_reply = _redis_reply;
        _redis_reply = NULL;
        return redis_reply;
    }

    RedisReplyHelper& operator =(const redisReply* redis_reply)
    {
        if (_redis_reply != NULL)
        {
            freeReplyObject((void*)_redis_reply);
        }

        _redis_reply = redis_reply;
        return *this;
    }

    RedisReplyHelper& operator =(const RedisReplyHelper& other)
    {
        if (_redis_reply != NULL)
        {
            freeReplyObject((void*)_redis_reply);
        }

        _redis_reply = other.detach();
        return *this;
    }

    const redisReply* operator ->() const
    {
        return _redis_reply;
    }

    std::ostream& operator <<(std::ostream& os)
    {
        os << _redis_reply;
        return os;
    }

private:
    mutable const redisReply* _redis_reply;
};

struct ErrorInfo
{
    std::string raw_errmsg;
    std::string errmsg;
    std::string errtype; // The type of error, such as: ERR, MOVED, WRONGTYPE, ...
    int errcode;

    ErrorInfo()
        : errcode(0)
    {
    }

    ErrorInfo(const std::string& raw_errmsg_, const std::string& errmsg_, const std::string& errtype_, int errcode_)
        : raw_errmsg(raw_errmsg_), errmsg(errmsg_), errtype(errtype_), errcode(errcode_)
    {
    }

    void clear()
    {
        errcode = 0;
        errtype.clear();
        errmsg.clear();
        raw_errmsg.clear();
    }
};

class CRedisException: public std::exception
{
public:
    // key maybe a binary value
    CRedisException(const struct ErrorInfo& errinfo, const char* file, int line, const std::string& node_ip=std::string("-"), uint16_t node_port=0, const std::string& command=std::string(""), const std::string& key=std::string("")) throw ();
    virtual ~CRedisException() throw () {}
    virtual const char* what() const throw ();
    int errcode() const { return _errinfo.errcode; }
    std::string str() const throw ();

    const char* file() const throw () { return _file.c_str(); }
    int line() const throw () { return _line; }
    const char* node_ip() const throw () { return _node_ip.c_str(); }
    uint16_t node_port() const throw () { return _node_port; }
    const std::string& command() const throw() { return _command; }
    const std::string& key() const throw() { return _key; }
    const std::string& errtype() const throw () { return _errinfo.errtype; }
    const std::string& raw_errmsg() const throw () { return _errinfo.raw_errmsg; }
    const ErrorInfo& get_errinfo() const throw() { return _errinfo; }

private:
    const ErrorInfo _errinfo;
    const std::string _file;
    const int _line;
    const std::string _node_ip;
    const uint16_t _node_port;
    std::string _command;
    std::string _key;
};

bool is_moved_error(const std::string& errtype);
bool is_wrongtype_error(const std::string& errtype);
bool is_clusterdown_error(const std::string& errtype);

// NOTICE: not thread safe
// A redis client than support redis cluster
//
// Recommended multithread solution:
// Use `__thread` to the global instance of CRedisClient for thread level.
//
// EXAMPLE:
// static __thread r3c::CRedisClient* stg_redis_client = NULL; // thread level gobal variable
// r3c::CRedisClient* get_redis_client()
// {
//     if (NULL == stg_redis_client)
//     {
//         stg_redis_client = new r3c::CRedisClient(REDIS_CLUSTER_NODES);
//         //pthread_cleanup_push(release_redis_client, NULL);
//     }
//     return stg_redis_client;
// }
//
// By calling pthread_cleanup_push() to registger a callback to release sg_redis_client when thread exits.
// EXAMPLE:
// void release_redis_client(void*)
// {
//     delete sg_redis_client;
//     stg_redis_client = NULL;
// }
struct RedisNode;
struct SlotInfo;
class CCommandArgs;

// NOTICE:
// 1) ALL keys and values can be binary.
// 2) DO NOT use the return values of any command in MULTI & EXEC transaction
// 3) ALWAYS set retry times to 0 of any command  in MULTI & EXEC transaction
class CRedisClient
{
public:
    // nodes - Redis cluster nodes separated by comma,
    //         e.g., 127.0.0.1:6379,127.0.0.1:6380,127.0.0.2:6379,127.0.0.3:6379,
    //         standalone mode if only one node, else cluster mode.
    //
    // In particular, the same nodes is allowed for cluster mode:
    // 127.0.0.1:6379,127.0.0.1:6379
    CRedisClient(
            const std::string& nodes,
            int connect_timeout_milliseconds=CONNECT_TIMEOUT_MILLISECONDS,
            int data_timeout_milliseconds=DATA_TIMEOUT_MILLISECONDS,
            int retry_sleep_milliseconds=RETRY_SLEEP_MILLISECONDS,
            const std::string& password=std::string(""),
            ReadPolicy read_policy = RP_ONLEY_MASTER
            ) throw (CRedisException);
    ~CRedisClient();

    // Returns true if parameter nodes of ctor is composed of two or more nodes,
    // or false when only a node for standlone mode.
    bool cluster_mode() const;

public:
    int list_nodes(std::vector<struct NodeInfo>* nodes_info) throw (CRedisException);

    // NOT SUPPORT CLUSTER
    //
    // Remove all keys from all databases.
    //
    // The time-complexity for this operation is O(N), N being the number of keys in all existing databases.
    void flushall() throw (CRedisException);

    // Marks the start of a transaction block. Subsequent commands will be queued for atomic execution using EXEC.
    //
    // NOTICE:
    // 1) DO NOT use the return values of any command in MULTI & EXEC transaction
    // 2) ALWAYS set retry times to 0 of any command  in MULTI & EXEC transaction
    // 3) In cluster mode, the keys of all commands should be same in MULTI & EXEC transaction
    void multi(const std::string& key, std::pair<std::string, uint16_t>* which=NULL);

    // Executes all previously queued commands in a transaction and restores the connection state to normal.
    const RedisReplyHelper exec(const std::string& key, std::pair<std::string, uint16_t>* which=NULL);

public: // KV
    // Set a key's time to live in seconds.
    // Time complexity: O(1)
    //
    // force_retry Not to retry if force_retry is false when error is network timeout.
    //
    // Returns true if the timeout was set, or false when key does not exist.
    bool expire(const std::string& key, uint32_t seconds, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Determine if a key exists.
    // Time complexity: O(1)
    // Returns true if the key exists, or false when the key does not exist.
    bool exists(const std::string& key, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Time complexity:
    // O(N) where N is the number of keys that will be removed.
    // When a key to remove holds a value other than a string,
    // the individual complexity for this key is O(M) where M is the number of elements in the list, set, sorted set or hash.
    // Removing a single key that holds a string value is O(1).
    //
    // Returns true, or false when key does not exist.
    bool del(const std::string& key, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Get the value of a key
    // Time complexity: O(1)
    // Returns false if key does not exist.
    bool get(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Set the string value of a key.
    // Time complexity: O(1)
    void set(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Set the value of a key, only if the does not exist.
    // Time complexity: O(1)
    // Returns true if the key was set, or false the key was not set.
    bool setnx(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);

    // Set the value and expiration of a key.
    // Time complexity: O(1)
    void setex(const std::string& key, const std::string& value, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);
    bool setnxex(const std::string& key, const std::string& value, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);

    // Get the values of all the given keys.
    //
    // Time complexity:
    // O(N) where N is the number of keys to retrieve.
    //
    // For every key that does not hold a string value or does not exist, the value will be empty string value.
    //
    // Returns the number of values.
    int mget(const std::vector<std::string>& keys, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Set multiple keys to multiple values.
    //
    // In cluster mode, mset guaranteed atomicity, or may partially success.
    //
    // Time complexity:
    // O(N) where N is the number of keys to set.
    int mset(const std::map<std::string, std::string>& kv_map, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Increment the integer value of a key by the given value.
    // Time complexity: O(1)
    // Returns the value of key after the increment.
    int64_t incrby(const std::string& key, int64_t increment, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);

    // Atomically increment and expire a key with given seconds.
    // Expiration is set only if the value returned by incrby is equal to expired_increment.
    //
    // NOTICE: Not support binary key and binary value.
    //
    // e.g.,
    // incrby(key, 1, 1, 10);
    int64_t incrby(const std::string& key, int64_t increment, int64_t expired_increment, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);

    // Same as incrby(key, increment, increment, expired_seconds, which, retry_times)
    int64_t incrby(const std::string& key, int64_t increment, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);

    // Determine the type stored at key.
    // Time complexity: O(1)
    bool key_type(const std::string& key, std::string* key_type, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Get the time to live for a key
    //
    // Time complexity: O(1)
    // Returns the remaining time to live of a key that has a timeout.
    int64_t ttl(const std::string& key, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // NOT SUPPORTED CLUSTER
    //
    // Incrementally iterate sorted sets elements and associated scores.
    //
    // Time complexity:
    // O(1) for every call. O(N) for a complete iteration,
    // including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection.
    //
    // Returns an updated cursor that the user needs to use as the cursor argument in the next call.
    int64_t scan(int64_t cursor, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    int64_t scan(int64_t cursor, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    int64_t scan(int64_t cursor, const std::string& pattern, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // If pattern is empty, then not using MATCH,
    // If count is 0, then not using COUNT
    int64_t scan(int64_t cursor, const std::string& pattern, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Execute a Lua script server side.
    //
    // NOTICE: Not support binary key and binary value, because eval is implemented by lua script.
    //
    // Time complexity: Depends on the script that is executed.
    const RedisReplyHelper eval(bool is_read_command, const std::string& key, const std::string& lua_scripts, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);
    const RedisReplyHelper eval(bool is_read_command, const std::string& key, const std::string& lua_scripts, const std::vector<std::string>& parameters, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);
    const RedisReplyHelper eval(const std::string& key, const std::string& lua_scripts, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);
    const RedisReplyHelper eval(const std::string& key, const std::string& lua_scripts, const std::vector<std::string>& parameters, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);
    const RedisReplyHelper evalsha(bool is_read_command, const std::string& key, const std::string& sha1, const std::vector<std::string>& parameters, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);
    const RedisReplyHelper evalsha(const std::string& key, const std::string& sha1, const std::vector<std::string>& parameters, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);

public: // HASH
    // Delete a hash field.
    // Time complexity: O(1)
    bool hdel(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Delete one or more hash fields.
    // Time complexity: O(N) where N is the number of fields to be removed.
    // Returns the number of fields that were removed from the hash, not including specified but non existing fields.
    int hdel(const std::string& key, const std::vector<std::string>& fields, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);
    int hmdel(const std::string& key, const std::vector<std::string>& fields, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Determinte if a hash field exists.
    // Time complexity: O(1)
    // Returns true if the hash contains field, or false when the hash does not contain field, or key does not exist.
    bool hexists(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Get the number of fields in a hash
    // Time complexity: O(1)
    // Returns number of fields in the hash, or 0 when key does not exist.
    int hlen(const std::string& key, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Set the string value of a hash field.
    // Time complexity: O(1)
    // Returns true if field is a new field in the hash and value was set, or false.
    bool hset(const std::string& key, const std::string& field, const std::string& value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);
    bool hsetex(const std::string& key, const std::string& field, const std::string& value, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Set the value of a hash field, only if the field does not exists.
    //
    // Time complexity: O(1)
    // Returns true if field is a new field in the hash and value was set,
    // or field already exists in the hash and no operation was performed.
    bool hsetnx(const std::string& key, const std::string& field, const std::string& value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);

    // NOTICE: Not support binary key and binary value.
    bool hsetnxex(const std::string& key, const std::string& field, const std::string& value, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);

    // Time complexity: O(1)
    // Returns true if exists, or false when field is not present in the hash or key does not exist.
    bool hget(const std::string& key, const std::string& field, std::string* value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Increment the integer value of a hash field by the given number.
    //
    // Time complexity: O(1)
    // Returns the value at field after the increment operation.
    int64_t hincrby(const std::string& key, const std::string& field, int64_t increment, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);

    // NOTICE: Not support binary key and binary value.
    void hincrby(const std::string& key, const std::vector<std::pair<std::string, int64_t> >& increments, std::vector<int64_t>* values=NULL, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);

    // NOTICE: Not support binary key and binary value.
    void hmincrby(const std::string& key, const std::vector<std::pair<std::string, int64_t> >& increments, std::vector<int64_t>* values=NULL, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);

    // Set multiple hash fields to multiple values.
    // Time complexity: O(N) where N is the number of fields being set.
    void hset(const std::string& key, const std::map<std::string, std::string>& map, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);
    void hmset(const std::string& key, const std::map<std::string, std::string>& map, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Get the values of all the given hash fields.
    // Time complexity: O(N) where N is the number of fields being requested.
    int hget(const std::string& key, const std::vector<std::string>& fields, std::map<std::string, std::string>* map, bool keep_null=false, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    int hmget(const std::string& key, const std::vector<std::string>& fields, std::map<std::string, std::string>* map, bool keep_null=false, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Get all the fields and values in a hash.
    // Time complexity: O(N) where N is the size of the hash.
    int hgetall(const std::string& key, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Time complexity: O(1)
    //
    // Returns the string length of the value associated with field,
    // or zero when field is not present in the hash or key does not exist at all.
    int hstrlen(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Get all the fields in a hash.
    // Time complexity: O(N) where N is the size of the hash.
    int hkeys(const std::string& key, std::vector<std::string>* fields, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Get all the values in a hash.
    // Time complexity: O(N) where N is the size of the hash.
    int hvals(const std::string& key, std::vector<std::string>* vals, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Incrementally iterate hash fields and associated values.
    //
    // Time complexity:
    // O(1) for every call. O(N) for a complete iteration,
    // including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection..
    //
    // Returns an updated cursor that the user needs to use as the cursor argument in the next call.
    int64_t hscan(const std::string& key, int64_t cursor, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    int64_t hscan(const std::string& key, int64_t cursor, int count, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    int64_t hscan(const std::string& key, int64_t cursor, const std::string& pattern, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // If pattern is empty, then not using MATCH,
    // If count is 0, then not using COUNT
    int64_t hscan(const std::string& key, int64_t cursor, const std::string& pattern, int count, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

public: // LIST
    // Get the length of a list
    // Time complexity: O(1)
    int llen(const std::string& key, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Remove and get the first element in a list.
    // Time complexity: O(1)
    bool lpop(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Prepend a value to a list.
    // Time complexity: O(1)
    int lpush(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Prepend one or multiple values to a list.
    // Time complexity: O(1)
    int lpush(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Get a range of elements from a list.
    //
    // Time complexity:
    // O(S+N) where S is the distance of start offset from HEAD for small lists,
    // from nearest end (HEAD or TAIL) for large lists;
    // and N is the number of elements in the specified range.
    //
    // Returns the number of elements in the specified range.
    int lrange(const std::string& key, int64_t start, int64_t end, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Trim a list to the specified range.
    // Time complexity:
    // O(N) where N is the number of elements to be removed by the operation.
    void ltrim(const std::string& key, int64_t start, int64_t end, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Remove and get the last element in a list.
    // Time complexity: O(1)
    bool rpop(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Append a value to a list.
    // Time complexity: O(1)
    // Returns the length of the list after the push operation.
    int rpush(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Append one or multiple values to a list.
    // Time complexity: O(1)
    // Returns the length of the list after the push operation.
    int rpush(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Inserts value at the tail of the list stored at key,
    // only if key already exists and holds a list. In contrary to RPUSH,
    // no operation will be performed when key does not yet exist.
    //
    // Time complexity: O(1)
    //
    // Returns the length of the list after the push operation.
    int rpushx(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);

public: // SET
    // Returns the number of elements that were added to the set,
    // not including all the elements already present into the set.
    int sadd(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);
    int sadd(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Returns the cardinality (number of elements) of the set, or 0 if key does not exist.
    int scard(const std::string& key, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    bool sismember(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    int smembers(const std::string& key, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Removes and returns a random elements from the set value store at key.
    // Time complexity: O(1)
    // Returns true if key exists, or false when key does not exist.
    bool spop(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Removes and returns one or more random elements from the set value store at key.
    // Time complexity: O(1)
    // Returns the number of removed elements.
    int spop(const std::string& key, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Returns true if key exists, or false when key does not exist.
    bool srandmember(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    // Returns number of values, or 0 when key does not exist.
    int srandmember(const std::string& key, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Remove a member from a set.
    // Time complexity: O(1)
    // Returns the number of members that were removed from the set, not including non existing members.
    int srem(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Remove one or more members from a set.
    // Time complexity: O(N) where N is the number of members to be removed.
    // Returns the number of members that were removed from the set, not including non existing members.
    int srem(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Incrementally iterate set elements.
    //
    // Time complexity:
    // O(1) for every call. O(N) for a complete iteration,
    // including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection..
    //
    // Returns an updated cursor that the user needs to use as the cursor argument in the next call.
    int64_t sscan(const std::string& key, int64_t cursor, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    int64_t sscan(const std::string& key, int64_t cursor, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    int64_t sscan(const std::string& key, int64_t cursor, const std::string& pattern, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // If pattern is empty, then not using MATCH,
    // If count is 0, then not using COUNT
    int64_t sscan(const std::string& key, int64_t cursor, const std::string& pattern, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    int64_t sscan(const std::string& key, int64_t cursor, const std::string& pattern, int count, std::set<std::string>* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

public: // ZSET
    // Removes the specified members from the sorted set stored at key. Non existing members are ignored.
    //
    // Time complexity:
    // O(M*log(N)) with N being the number of elements in the sorted set and M the number of elements to be removed.
    //
    // Returns the number of members removed from the sorted set, not including non existing members.
    int zrem(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);
    int zrem(const std::string& key, const std::vector<std::string>& fields, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Adds all the specified members with the specified scores to the sorted set stored at key.
    // If key does not exist, a new sorted set with the specified members as sole members is created,
    // like if the sorted set was empty.
    //
    // Time complexity:
    // O(log(N)) for each item added, where N is the number of elements in the sorted set.
    //
    // Returns The number of elements added to the sorted sets,
    // not including elements already existing for which the score was updated.
    int zadd(const std::string& key, const std::string& field, int64_t score, ZADDFLAG flag=Z_NS, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);
    int zadd(const std::string& key, const std::map<std::string, int64_t>& map, ZADDFLAG flag=Z_NS, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Get the number of members in a sorted set.
    // Time complexity: O(1)
    // Returns the sorted set cardinality (number of elements) of the sorted set stored at key.
    int64_t zcard(const std::string& key, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Count the members in a sorted set with scores within the given values.
    //
    // Time complexity:
    // O(log(N)) with N being the number of elements in the sorted set.
    // Returns the number of elements in the sorted set at key with a score between min and max.
    int64_t zcount(const std::string& key, int64_t min, int64_t max , std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Increment the score of a member in a sorted set.
    //
    // Time complexity:
    // O(log(N)) where N is the number of elements in the sorted set.
    int64_t zincrby(const std::string& key, const std::string& field, int64_t increment, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=false) throw (CRedisException);

    // Return a range of members in a sorted set by index.
    //
    // Time complexity:
    // O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned.
    int zrange(const std::string& key, int64_t start, int64_t end, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Return a range of members in a sorted set by index, with scores ordered from high to low.
    //
    // Time complexity:
    // O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned.
    int zrevrange(const std::string& key, int64_t start, int64_t end, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Get all the elements in the sorted set at key with a score between min and max
    // (including elements with score equal to min or max).
    // The elements are considered to be ordered from low to high scores.
    //
    // The elements having the same score are returned in lexicographical order
    // (this follows from a property of the sorted set implementation in Redis and does not involve further computation).
    //
    // Time complexity:
    // O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned.
    // If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
    //
    // Return the number of elements with a score between min and max (including elements with score equal to min or max).
    int zrangebyscore(const std::string& key, int64_t min, int64_t max, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Get all the elements in the sorted set at key with a score between max and min
    // (including elements with score equal to max or min).
    // In contrary to the default ordering of sorted sets,
    // for this command the elements are considered to be ordered from high to low scores.
    //
    // The elements having the same score are returned in reverse lexicographical order.
    //
    // Time complexity:
    // O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned.
    // If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
    //
    // Return the number of elements with a score between max and min (including elements with score equal to max or min).
    int zrevrangebyscore(const std::string& key, int64_t max, int64_t min, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Return a range of members in a sorted set by score with scores ordered from low to high.
    int zrangebyscore(const std::string& key, int64_t min, int64_t max, int64_t offset, int64_t count, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    // Return a range of members in a sorted set by score with scores ordered from high to low.
    int zrevrangebyscore(const std::string& key, int64_t max, int64_t min, int64_t offset, int64_t count, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Removes all elements in the sorted set stored at key with rank between start and stop.
    // Both start and stop are 0 -based indexes with 0 being the element with the lowest score.
    // These indexes can be negative numbers, where they indicate offsets starting at the element with the highest score.
    // For example:
    // -1 is the element with the highest score,
    // -2 the element with the second highest score and so forth.
    //
    // Time complexity:
    // O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements removed by the operation.
    //
    // Return the number of elements removed.
    int zremrangebyrank(const std::string& key, int64_t start, int64_t end, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES, bool force_retry=true) throw (CRedisException);

    // Determine the index of a member in a sorted set.
    // Time complexity: O(log(N))
    // Return -1 if field not exists
    int zrank(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Determine the index of a member in a sorted set, with scores ordered from high to low.
    int zrevrank(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Get the score associated with the given memer in a sorted set.
    // Time complexity: O(1)
    // Return -1 if field not exists
    int64_t zscore(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // Incrementally iterate sorted sets elements and associated scores.
    //
    // Time complexity:
    // O(1) for every call. O(N) for a complete iteration,
    // including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection..
    //
    // Returns an updated cursor that the user needs to use as the cursor argument in the next call.
    int64_t zscan(const std::string& key, int64_t cursor, std::vector<std::pair<std::string, int64_t> >* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    int64_t zscan(const std::string& key, int64_t cursor, int count, std::vector<std::pair<std::string, int64_t> >* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);
    int64_t zscan(const std::string& key, int64_t cursor, const std::string& pattern, std::vector<std::pair<std::string, int64_t> >* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

    // If pattern is empty, then not using MATCH,
    // If count is 0, then not using COUNT
    int64_t zscan(const std::string& key, int64_t cursor, const std::string& pattern, int count, std::vector<std::pair<std::string, int64_t> >* values, std::pair<std::string, uint16_t>* which=NULL, int retry_times=RETRY_TIMES) throw (CRedisException);

public:
    // Standlone: key should be empty
    // Cluse mode: key used to locate node
    //
    // Will ignore force_retry if is_read_command is true
    // If network timeout, the result of write is not uncertain, maybe succes or failure.
    // Not retry if force_retry is false when is_read_command is false.
    const RedisReplyHelper redis_command(bool is_read_command, bool force_retry, int retry_times, const std::string& key, const CCommandArgs& command_args, std::pair<std::string, uint16_t>* which);

private:
    void init();
    void init_standlone();
    void init_cluster();

private:
    void free_slots_info();
    void reset_slots_info(int slot);
    void update_slot_info(int slot, const std::pair<std::string, uint16_t>& node);

private:
    void free_redis_nodes();
    redisContext* connect_redis_node(int slot, const std::pair<std::string, uint16_t>& node, struct ErrorInfo* errinfo) const;
    void close_redis_node(struct RedisNode*& redis_node);
    struct RedisNode* find_redis_node(const std::pair<std::string, uint16_t>& node);
    struct RedisNode* get_redis_node(int slot, bool is_read_command, bool* is_node_of_slot, struct ErrorInfo* errinfo);
    struct RedisNode* add_redis_node(const std::pair<std::string, uint16_t>& node, redisContext* redis_context);
    bool get_nodes_info(std::vector<struct NodeInfo>* nodes_info, struct ErrorInfo* errinfo, int i, redisContext* redis_context, const std::pair<std::string, uint16_t>& node);
    bool get_slave_nodes(redisContext* redis_context, std::vector<std::pair<std::string, uint16_t> >* nodes);

    // Called by: redis_command
    void extract_errtype(const redisReply* redis_reply, std::string* errtype);

public:
    // Called by: get,hget,key_type,lpop,rpop,srandmember
    bool get_value(const redisReply* redis_reply, std::string* value);

    // Called by: hkeys,hvals,lrange,mget,scan,smembers,spop,srandmember,sscan
    int get_values(const redisReply* redis_reply, std::vector<std::string>* values);

    // Called by: sscan
    int get_values(const redisReply* redis_reply, std::set<std::string>* values);

    // Called by: zrange & zrevrange & zrangebyscore & zrevrangebyscore & zscan
    int get_values(const redisReply* redis_reply, std::vector<std::pair<std::string, int64_t> >* vec, bool withscores);

    // Called by: hgetall & hscan
    int get_values(const redisReply* redis_reply, std::map<std::string, std::string>* map);

    // Called by: hmget
    int get_values(const redisReply* redis_reply, const std::vector<std::string>& fields, bool keep_null, std::map<std::string, std::string>* map);

    // Called by: hmincrby
    int get_values(const redisReply* redis_reply, std::vector<int64_t>* values);

private:
    std::string _nodes_string;
    int _connect_timeout_milliseconds;
    int _data_timeout_milliseconds;
    int _retry_times;
    int _retry_sleep_milliseconds;
    std::string _password;
    ReadPolicy _read_policy;

private:
    std::map<std::pair<std::string, uint16_t>, struct RedisNode*> _redis_contexts;
    std::vector<std::pair<std::string, uint16_t> > _nodes; // first is IP, second is port
    std::vector<struct NodeInfo> _nodes_info;
    std::vector<struct SlotInfo*> _slots_info; // subscript is slot
};

} // namespace r3c {
#endif // REDIS_CLUSTER_CLIENT_H
