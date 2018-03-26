#include "r3c.h"
#include "utils.h"

#define THROW_REDIS_EXCEPTION(errinfo) throw CRedisException(errinfo, __FILE__, __LINE__)
#define THROW_REDIS_EXCEPTION_WITH_NODE(errinfo, node_ip, node_port) throw CRedisException(errinfo, __FILE__, __LINE__, node_ip, node_port)
#define THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(errinfo, node_ip, node_port, command, key) throw CRedisException(errinfo, __FILE__, __LINE__, node_ip, node_port, command, key)

namespace r3c {

#if 0
static LOG_WRITE g_error_log = r3c_log_write;
static LOG_WRITE g_info_log = r3c_log_write;
static LOG_WRITE g_debug_log = r3c_log_write;
#else
static LOG_WRITE g_error_log = null_log_write;
static LOG_WRITE g_info_log = null_log_write;
static LOG_WRITE g_debug_log = null_log_write;
#endif

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

enum
{
    CLUSTER_SLOTS = 16384, // number of slots, defined in cluster.h
    RETRY_FREQUENCY = 5
};

std::string zaddflag2str(ZADDFLAG zaddflag)
{
    std::string zaddflag_str;

    switch (zaddflag)
    {
    case Z_XX:
        zaddflag_str = "XX";
        break;
    case Z_NX:
        zaddflag_str = "NX";
        break;
    case Z_CH:
        zaddflag_str = "CH";
        break;
    default:
        break;
    }

    return zaddflag_str;
}

std::string NodeInfo::str() const
{
    return format_string("node://%s/%s:%d/%s", id.c_str(), ip.c_str(), port, flags.c_str());
}

bool NodeInfo::is_master() const
{
    const std::string::size_type pos = flags.find("master");
    return (pos != std::string::npos);
}

bool NodeInfo::is_slave() const
{
    const std::string::size_type pos = flags.find("slave");
    return (pos != std::string::npos);
}

bool NodeInfo::is_fail() const
{
    const std::string::size_type pos = flags.find("fail");
    return (pos != std::string::npos);
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

static void debug_reply(const char* command, const char* key, int slot, const redisReply* redis_reply, const std::pair<std::string, uint16_t>& node)
{
    if (REDIS_REPLY_STRING == redis_reply->type)
        (*g_debug_log)("[%s:%d][STRING][%s][SLOT:%d]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d)%s\n", __FILE__, __LINE__, command, slot, key, node.first.c_str(), node.second, redis_reply->type, redis_reply->str);
    else if (REDIS_REPLY_INTEGER == redis_reply->type)
        (*g_debug_log)("[%s:%d][INTEGER][%s][SLOT:%d]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d)%lld\n", __FILE__, __LINE__, command, slot, key, node.first.c_str(), node.second, redis_reply->type, redis_reply->integer);
    else if (REDIS_REPLY_ARRAY == redis_reply->type)
        (*g_debug_log)("[%s:%d][ARRAY][%s][SLOT:%d]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d)%zd\n", __FILE__, __LINE__, command, slot, key, node.first.c_str(), node.second, redis_reply->type, redis_reply->elements);
    else if (REDIS_REPLY_NIL == redis_reply->type)
        (*g_debug_log)("[%s:%d][NIL][%s][SLOT:%d]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d)%s\n", __FILE__, __LINE__, command, slot, key, node.first.c_str(), node.second, redis_reply->type, redis_reply->str);
    else if (REDIS_REPLY_ERROR == redis_reply->type)
        (*g_debug_log)("[%s:%d][ERROR][%s][SLOT:%d]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d)(%d)%s\n", __FILE__, __LINE__, command, slot, key, node.first.c_str(), node.second, redis_reply->type, redis_reply->integer, redis_reply->str);
    else if (REDIS_REPLY_STATUS == redis_reply->type)
        (*g_debug_log)("[%s:%d][STATUS][%s][SLOT:%d]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d)%s\n", __FILE__, __LINE__, command, slot, key, node.first.c_str(), node.second, redis_reply->type, redis_reply->str);
    else
        (*g_debug_log)("[%s:%d][->%d][%s][SLOT:%d]["PRINT_COLOR_GREEN"KEY:%s"PRINT_COLOR_NONE"][%s:%d]reply: (%d)%s\n", __FILE__, __LINE__, redis_reply->type, command, slot, key, node.first.c_str(), node.second, redis_reply->type, redis_reply->str);
}

////////////////////////////////////////////////////////////////////////////////

class CCommandArgs
{
public:
    CCommandArgs();
    ~CCommandArgs();
    void set_key(const std::string& key);
    void add_arg(const std::string& arg);
    void add_arg(int32_t arg);
    void add_arg(uint32_t arg);
    void add_arg(int64_t arg);
    void add_args(const std::vector<std::string>& args);
    void add_args(const std::map<std::string, std::string>& map);
    void add_args(const std::map<std::string, int64_t>& map, bool reverse);
    void final();
    int get_argc() const;
    const char** get_argv() const;
    const size_t* get_argvlen() const;
    const char* get_command() const;
    const char* get_key() const;
    size_t get_command_length() const;
    size_t get_key_length() const;
    std::string get_command_str() const;
    std::string get_key_str() const;

private:
    std::string _command;
    std::string _key;
    std::vector<std::string> _args;
    int _argc;
    char** _argv;
    size_t* _argvlen;
};

CCommandArgs::CCommandArgs()
    : _argc(0), _argv(NULL), _argvlen(NULL)
{
}

CCommandArgs::~CCommandArgs()
{
    delete []_argvlen;
    for (int i=0; i<_argc; ++i)
        delete []_argv[i];
    delete []_argv;
}

void CCommandArgs::set_key(const std::string& key)
{
    _key = key;
}

void CCommandArgs::add_arg(const std::string& arg)
{
    _args.push_back(arg);
}

void CCommandArgs::add_arg(int32_t arg)
{
    _args.push_back(any2string(arg));
}

void CCommandArgs::add_arg(uint32_t arg)
{
    _args.push_back(any2string(arg));
}

void CCommandArgs::add_arg(int64_t arg)
{
    _args.push_back(any2string(arg));
}

void CCommandArgs::add_args(const std::vector<std::string>& args)
{
    for (std::vector<std::string>::size_type i=0; i<args.size(); ++i)
    {
        const std::string& arg = args[i];
        add_arg(arg);
    }
}

void CCommandArgs::add_args(const std::map<std::string, std::string>& map)
{
    for (std::map<std::string, std::string>::const_iterator iter=map.begin(); iter!=map.end(); ++iter)
    {
        add_arg(iter->first);
        add_arg(iter->second);
    }
}

void CCommandArgs::add_args(const std::map<std::string, int64_t>& map, bool reverse)
{
    for (std::map<std::string, int64_t>::const_iterator iter=map.begin(); iter!=map.end(); ++iter)
    {
        if (!reverse)
        {
            add_arg(iter->first);
            add_arg(any2string(iter->second));
        }
        else
        {
            add_arg(iter->second);
            add_arg(any2string(iter->first));
        }
    }
}

void CCommandArgs::final()
{
    _argc = static_cast<int>(_args.size());
    _argv = new char*[_argc];
    _argvlen = new size_t[_argc];

    for (int i=0; i<_argc; ++i)
    {
        _argvlen[i] = _args[i].size();
        _argv[i] = new char[_argvlen[i]+1];
        memcpy(_argv[i], _args[i].c_str(), _argvlen[i]); // Support binary key&value.
        _argv[i][_argvlen[i]] = '\0';
    }
}

inline int CCommandArgs::get_argc() const
{
    return _argc;
}

inline const char** CCommandArgs::get_argv() const
{
    return (const char**)_argv;
}

inline const size_t* CCommandArgs::get_argvlen() const
{
    return _argvlen;
}

inline const char* CCommandArgs::get_command() const
{
    return _argv[0];
}

inline const char* CCommandArgs::get_key() const
{
    return !_key.empty()? _key.c_str(): _argv[1];
}

inline size_t CCommandArgs::get_command_length() const
{
    return _argvlen[0];
}

inline size_t CCommandArgs::get_key_length() const
{
    return _argvlen[1];
}

std::string CCommandArgs::get_command_str() const
{
    return std::string(get_command(), get_command_length());
}

std::string CCommandArgs::get_key_str() const
{
    return std::string(get_key(), get_key_length());
}

////////////////////////////////////////////////////////////////////////////////

struct RedisNode
{
public:
    redisContext* context;
    std::pair<std::string, uint16_t> ip_and_port;
    int failed;
    int index;

private:
    bool master; // true if is master node, or false if is slave node

public:
    RedisNode(int index_)
        : context(NULL), failed(0), index(index_), master(false)
    {
    }

    bool is_master() const { return master; }
    bool is_slave() const { return !master; }

    void become_master()
    {
        master = true;
    }

    void become_slave()
    {
        master = false;
    }
};

struct SlotInfo
{
    int slot;
    struct RedisNode* master_node;
    std::vector<struct RedisNode*> slave_nodes;

    SlotInfo(int slot_)
        : slot(slot_), master_node(NULL)
    {
    }
};

CRedisException::CRedisException(const struct ErrorInfo& errinfo, const char* file, int line, const std::string& node_ip, uint16_t node_port, const std::string& command, const std::string& key) throw ()
    :  _errinfo(errinfo), _file(file), _line(line), _node_ip(node_ip), _node_port(node_port), _command(command), _key(key)
{
}

const char* CRedisException::what() const throw()
{
    return _errinfo.errmsg.c_str();
}

std::string CRedisException::str() const throw ()
{
    return format_string("redis://%s:%d/CMD:%s/KEY:%.*s/%s/(%d)%s@%s:%d", _node_ip.c_str(), _node_port, _command.c_str(), _key.length(), _key.c_str(), _errinfo.errtype.c_str(), _errinfo.errcode, _errinfo.errmsg.c_str(), _file.c_str(), _line);
}

bool is_moved_error(const std::string& errtype)
{
    return errtype == "MOVED";
}

bool is_wrongtype_error(const std::string& errtype)
{
    return errtype == "WRONGTYPE";
}

bool is_clusterdown_error(const std::string& errtype)
{
    return errtype == "CLUSTERDOWN";
}

CRedisClient::CRedisClient(const std::string& nodes, int connect_timeout_milliseconds, int data_timeout_milliseconds, int retry_sleep_milliseconds, const std::string& password, ReadPolicy read_policy) throw (CRedisException)
            : _nodes_string(nodes),
              _connect_timeout_milliseconds(connect_timeout_milliseconds),
              _data_timeout_milliseconds(data_timeout_milliseconds),
              _retry_times(RETRY_TIMES),
              _retry_sleep_milliseconds(retry_sleep_milliseconds),
              _password(password),
              _read_policy(read_policy)
{
    init();
}

CRedisClient::~CRedisClient()
{
    free_slots_info();
    free_redis_nodes();
}

bool CRedisClient::cluster_mode() const
{
    return _nodes.size() > 1;
}

int CRedisClient::list_nodes(std::vector<struct NodeInfo>* nodes_info) throw (CRedisException)
{
    struct ErrorInfo errinfo;
    int i = 0;

    for (std::map<std::pair<std::string, uint16_t>, struct RedisNode*>::iterator iter=_redis_contexts.begin(); iter!=_redis_contexts.end(); ++iter)
    {
        const std::pair<std::string, uint16_t>& node = iter->first;
        struct RedisNode* redis_node = iter->second;

        if (redis_node->context != NULL)
        {
            if (get_nodes_info(nodes_info, &errinfo, i++, redis_node->context, node))
            {
                break;
            }
        }
    }
    if (nodes_info->empty())
    {
        THROW_REDIS_EXCEPTION(errinfo);
    }

    return static_cast<int>(nodes_info->size());
}

void CRedisClient::flushall() throw (CRedisException)
{
    const int retry_times = RETRY_TIMES;
    const std::string key;
    CCommandArgs cmd_args;
    cmd_args.add_arg("FLUSHALL");
    cmd_args.final();

    // Simple string reply (REDIS_REPLY_STATUS)
    redis_command(false, true, retry_times, key, cmd_args, NULL);
}

void CRedisClient::multi(const std::string& key, std::pair<std::string, uint16_t>* which)
{
    const int retry_times = 0;
    const bool force_retry = false;
    CCommandArgs cmd_args;
    cmd_args.set_key(key);
    cmd_args.add_arg("MULTI");
    cmd_args.final();

    if (cluster_mode())
    {
        // 如果是集群模式，
        // MULTI必须在目录slot节点上执行
        // 调用一次ttl的目的是建立slot和目标节点的关系
        ttl(key);
    }

    // Simple string reply (REDIS_REPLY_STATUS):
    // always OK.
    redis_command(false, force_retry, retry_times, key, cmd_args, which);
}

const RedisReplyHelper CRedisClient::exec(const std::string& key, std::pair<std::string, uint16_t>* which)
{
    const int retry_times = 0;
    const bool force_retry = false;
    CCommandArgs cmd_args;
    cmd_args.set_key(key);
    cmd_args.add_arg("EXEC");
    cmd_args.final();

    // Array reply:
    // each element being the reply to each of the commands in the atomic transaction.
    return redis_command(false, force_retry, retry_times, key, cmd_args, which);
}

//
// KEY/VALUE
//

// Time complexity: O(1)
// EXPIRE key seconds
bool CRedisClient::expire(const std::string& key, uint32_t seconds, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("EXPIRE");
    cmd_args.add_arg(key);
    cmd_args.add_arg(seconds);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if the timeout was set.
    // 0 if key does not exist.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// Time complexity: O(1)
// EXISTS key [key ...]
bool CRedisClient::exists(const std::string& key, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("EXISTS");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if the key exists.
    // 0 if the key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// Time complexity:
// O(N) where N is the number of keys that will be removed.
// When a key to remove holds a value other than a string,
// the individual complexity for this key is O(M) where M is the number of elements in the list, set, sorted set or hash.
// Removing a single key that holds a string value is O(1).
bool CRedisClient::del(const std::string& key, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("DEL");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply:
    // The number of keys that were removed.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// GET key
// Time complexity: O(1)
bool CRedisClient::get(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("GET");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Bulk string reply:
    // the value of key, or nil when key does not exist.
    RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return false;
    if (REDIS_REPLY_STRING == redis_reply->type)
        return get_value(redis_reply.get(), value);
    return true;
}

// SET key value [EX seconds] [PX milliseconds] [NX|XX]
// Time complexity: O(1)
void CRedisClient::set(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SET");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Simple string reply (REDIS_REPLY_STATUS):
    // OK if SET was executed correctly.

    // Null reply: a Null Bulk Reply is returned if the SET operation was not performed
    // because the user specified the NX or XX option but the condition was not met.
    //
    // OK: redis_reply->str
    redis_command(false, force_retry, retry_times, key, cmd_args, which);
}

// Time complexity: O(1)
// SETNX key value
bool CRedisClient::setnx(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SETNX");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if the key exists.
    // 0 if the key does not exist.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// Time complexity: O(1)
// SETEX key seconds value
void CRedisClient::setex(const std::string& key, const std::string& value, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SETEX");
    cmd_args.add_arg(key);
    cmd_args.add_arg(expired_seconds);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Simple string reply (REDIS_REPLY_STATUS)
    //
    // OK: redis_reply->str
    redis_command(false, force_retry, retry_times, key, cmd_args, which);
}

bool CRedisClient::setnxex(const std::string& key, const std::string& value, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SET");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.add_arg("EX");
    cmd_args.add_arg(expired_seconds);
    cmd_args.add_arg("NX");
    cmd_args.final();

    // Simple string reply (REDIS_REPLY_STATUS):
    // OK if SET was executed correctly.
    //
    // Null reply: a Null Bulk Reply is returned if the SET operation was not performed
    // because the user specified the NX or XX option but the condition was not met.
    //
    // OK: redis_reply->str
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    return redis_reply->type != REDIS_REPLY_NIL;
}

// Time complexity: O(N) where N is the number of keys to retrieve.
// MGET key [key ...]
int CRedisClient::mget(const std::vector<std::string>& keys, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    values->clear();

    if (!cluster_mode())
    {
        const std::string key;
        CCommandArgs cmd_args;
        cmd_args.add_arg("MGET");
        cmd_args.add_args(keys);
        cmd_args.final();

        // Array reply:
        // list of values at the specified keys.
        //
        // For every key that does not hold a string value or does not exist,
        // the special value nil is returned. Because of this, the operation never fails.
        const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
        if (REDIS_REPLY_ARRAY == redis_reply->type)
            return get_values(redis_reply.get(), values);
        return 0;
    }
    else
    {
        values->resize(keys.size());

        try
        {
            for (std::vector<std::string>::size_type i=0; i<keys.size(); ++i)
            {
                const std::string& key = keys[i];
                std::string& value = (*values)[i];
                get(key, &value, NULL, retry_times);
            }
        }
        catch (CRedisException&)
        {
            values->clear();
            throw;
        }

        return static_cast<int>(values->size());
    }
}

// Time complexity:
// O(N) where N is the number of keys to set.
// MSET key value [key value ...]
int CRedisClient::mset(const std::map<std::string, std::string>& kv_map, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    int success = 0;

    if (!cluster_mode())
    {
        const std::string key;
        CCommandArgs cmd_args;
        cmd_args.add_arg("MSET");
        cmd_args.add_args(kv_map);
        cmd_args.final();

        // Simple string reply:
        // always OK since MSET can't fail.
        redis_command(false, force_retry, retry_times, key, cmd_args, which);
        success = static_cast<int>(kv_map.size());
    }
    else
    {
        for (std::map<std::string, std::string>::const_iterator iter=kv_map.begin(); iter!=kv_map.end(); ++iter)
        {
            const std::string& key = iter->first;
            const std::string& value = iter->second;
            set(key, value, which ,retry_times);
            ++success;
        }
    }

    return success;
}

// Time complexity: O(1)
// INCRBY key increment
int64_t CRedisClient::incrby(const std::string& key, int64_t increment, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("INCRBY");
    cmd_args.add_arg(key);
    cmd_args.add_arg(increment);
    cmd_args.final();

    // Integer reply:
    // the value of key after the increment
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return redis_reply->integer;
    return 0;
}

int64_t CRedisClient::incrby(const std::string& key, int64_t increment, int64_t expired_increment, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    const std::string& lua_scripts = format_string(
            "local n; n=redis.call('incrby','%s','%" PRId64"'); if (n==%" PRId64") then redis.call('expire', '%s', '%u') end; return n;",
            key.c_str(), increment, expired_increment, key.c_str(), expired_seconds);
    const RedisReplyHelper redis_reply = eval(false, key, lua_scripts, which, retry_times, force_retry);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int64_t>(redis_reply->integer);
    return 0;
}

int64_t CRedisClient::incrby(const std::string& key, int64_t increment, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    const int64_t expired_increment = increment;
    return incrby(key, increment, expired_increment, expired_seconds, which, retry_times, force_retry);
}

bool CRedisClient::key_type(const std::string& key, std::string* key_type, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("TYPE");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Simple string reply (REDIS_REPLY_STATUS):
    // type of key, or none when key does not exist.
    //
    // (gdb) p *redis_reply._redis_reply
    // $1 = {type = 5, integer = 0, len = 6, str = 0x742b50 "string", elements = 0, element = 0x0}
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    return get_value(redis_reply.get(), key_type);
}

int64_t CRedisClient::ttl(const std::string& key, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("TTL");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply:
    // TTL in seconds, or a negative value in order to signal an error
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return redis_reply->integer;
    return 0;
}

// O(1) for every call. O(N) for a complete iteration,
// including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection.
//
// SCAN cursor [MATCH pattern] [COUNT count]
int64_t CRedisClient::scan(int64_t cursor, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    return scan(cursor, std::string(""), 0, values, which, retry_times);
}

// SCAN cursor [MATCH pattern] [COUNT count]
int64_t CRedisClient::scan(int64_t cursor, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    return scan(cursor, std::string(""), count, values, which, retry_times);
}

// SCAN cursor [MATCH pattern] [COUNT count]
int64_t CRedisClient::scan(int64_t cursor, const std::string& pattern, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    return scan(cursor, pattern, 0, values, which, retry_times);
}

// SCAN cursor [MATCH pattern] [COUNT count]
int64_t CRedisClient::scan(int64_t cursor, const std::string& pattern, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    const std::string key;
    CCommandArgs cmd_args;
    cmd_args.set_key("");
    cmd_args.add_arg("SCAN");
    cmd_args.add_arg(cursor);
    if (!pattern.empty())
    {
        cmd_args.add_arg("MATCH");
        cmd_args.add_arg(pattern);
    }
    if (count > 0)
    {
        cmd_args.add_arg("COUNT");
        cmd_args.add_arg(count);
    }
    cmd_args.final();

    // return value is an array of two values:
    // the first value is the new cursor to use in the next call, the second value is an array of elements.
    //
    // *EMPTY*:
    // (gdb) p *redis_reply
    // $1 = {type = 2, integer = 0, len = 0, str = 0x0, elements = 2, element = 0x63e450}
    // (gdb) p *redis_reply->element[0]
    // $3 = {type = 1, integer = 0, len = 1, str = 0x63e6f0 "0", elements = 0, element = 0x0}
    // (gdb) p *redis_reply->element[1]
    // $4 = {type = 2, integer = 0, len = 0, str = 0x0, elements = 0, element = 0x0}
    //
    // *NON EMPTY*:
    //
    // redis-cli set k1 1
    // redis-cli set k2 2
    // redis-cli set k3 3
    //
    // (gdb) p *redis_reply
    // $5 = {type = 2, integer = 0, len = 0, str = 0x0, elements = 2, element = 0x63e450}
    // (gdb) p *redis_reply->element[0]
    // $6 = {type = 1, integer = 0, len = 1, str = 0x63e720 "1", elements = 0, element = 0x0}
    // (gdb) p *redis_reply->element[1]
    // $7 = {type = 2, integer = 0, len = 0, str = 0x0, elements = 3, element = 0x63e780}
    // (gdb) p *redis_reply->element[1]->element[0]
    // $8 = {type = 1, integer = 0, len = 2, str = 0x63e820 "k2", elements = 0, element = 0x0}
    // (gdb) p *redis_reply->element[1]->element[1]
    // $9 = {type = 1, integer = 0, len = 2, str = 0x63e880 "k3", elements = 0, element = 0x0}
    // (gdb) p *redis_reply->element[1]->element[2]
    // $10 = {type = 1, integer = 0, len = 2, str = 0x63e8e0 "k1", elements = 0, element = 0x0}
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
    {
        get_values(redis_reply->element[1], values);
        return static_cast<int64_t>(atoll(redis_reply->element[0]->str));
    }
    return 0;
}

//
// EVAL
//

// Time complexity: Depends on the script that is executed.
// EVAL script numkeys key [key ...] arg [arg ...]
//
// e.g.
// eval("r3c_k1", "local v=redis.call('set','r3c_k1','123');return v;");
// eval("r3c_k1", "local v=redis.call('get','r3c_k1');return v;");
const RedisReplyHelper CRedisClient::eval(bool is_read_command, const std::string& key, const std::string& lua_scripts, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    const int numkeys = 1;
    CCommandArgs cmd_args;
    cmd_args.set_key(key);
    cmd_args.add_arg("EVAL");
    cmd_args.add_arg(lua_scripts);
    cmd_args.add_arg(numkeys);
    cmd_args.add_arg(key);
    cmd_args.final();

    return redis_command(is_read_command, force_retry, retry_times, key, cmd_args, which);
}

const RedisReplyHelper CRedisClient::eval(bool is_read_command, const std::string& key, const std::string& lua_scripts, const std::vector<std::string>& parameters, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    const int numkeys = 1;
    CCommandArgs cmd_args;
    cmd_args.set_key(key);
    cmd_args.add_arg("EVAL");
    cmd_args.add_arg(lua_scripts);
    cmd_args.add_arg(numkeys);
    cmd_args.add_arg(key);
    cmd_args.add_args(parameters);
    cmd_args.final();

    return redis_command(is_read_command, force_retry, retry_times, key, cmd_args, which);
}

const RedisReplyHelper CRedisClient::eval(const std::string& key, const std::string& lua_scripts, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    return eval(false, key, lua_scripts, which, retry_times, force_retry);
}

const RedisReplyHelper CRedisClient::eval(const std::string& key, const std::string& lua_scripts, const std::vector<std::string>& parameters, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    return eval(false, key, lua_scripts, parameters, which, retry_times, force_retry);
}

const RedisReplyHelper CRedisClient::evalsha(bool is_read_command, const std::string& key, const std::string& sha1, const std::vector<std::string>& parameters, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    const int numkeys = 1;
    CCommandArgs cmd_args;
    cmd_args.set_key(key);
    cmd_args.add_arg("EVALSHA");
    cmd_args.add_arg(sha1);
    cmd_args.add_arg(numkeys);
    cmd_args.add_arg(key);
    cmd_args.add_args(parameters);
    cmd_args.final();

    return redis_command(is_read_command, force_retry, retry_times, key, cmd_args, which);
}

const RedisReplyHelper CRedisClient::evalsha(const std::string& key, const std::string& sha1, const std::vector<std::string>& parameters, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    return evalsha(false, key, sha1, parameters, which, retry_times, force_retry);
}

//
// HASH
//

// Time complexity: O(N) where N is the number of fields to be removed.
// HDEL key field [field ...]
bool CRedisClient::hdel(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HDEL");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // Integer reply:
    // the number of fields that were removed from the hash, not including specified but non existing fields.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return false;
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

int CRedisClient::hdel(const std::string& key, const std::vector<std::string>& fields, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    return hmdel(key, fields, which, retry_times, force_retry);
}

int CRedisClient::hmdel(const std::string& key, const std::vector<std::string>& fields, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HDEL");
    cmd_args.add_arg(key);
    cmd_args.add_args(fields);
    cmd_args.final();

    // Integer reply:
    // the number of fields that were removed from the hash, not including specified but non existing fields.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return 0;
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
// HEXISTS key field
bool CRedisClient::hexists(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HEXISTS");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if the hash contains field.
    // 0 if the hash does not contain field, or key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// Time complexity: O(1)
// HLEN key
int CRedisClient::hlen(const std::string& key, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HLEN");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply: number of fields in the hash, or 0 when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
// HSET key field value
bool CRedisClient::hset(const std::string& key, const std::string& field, const std::string& value, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HSET");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if field is a new field in the hash and value was set.
    // 0 if field already exists in the hash and the value was updated.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

bool CRedisClient::hsetex(const std::string& key, const std::string& field, const std::string& value, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    const std::string& lua_scripts = format_string(
            "local n; n=redis.call('hset','%s','%s','%s'); if (n>0) then redis.call('expire', '%s', '%u') end; return n;",
            key.c_str(), field.c_str(), value.c_str(), key.c_str(), expired_seconds);
    const RedisReplyHelper redis_reply = eval(false, key, lua_scripts, which, retry_times, force_retry);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// HSETNX key field value
// Time complexity: O(1)
bool CRedisClient::hsetnx(const std::string& key, const std::string& field, const std::string& value, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HSETNX");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if field is a new field in the hash and value was set.
    // 0 if field already exists in the hash and no operation was performed.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

bool CRedisClient::hsetnxex(const std::string& key, const std::string& field, const std::string& value, uint32_t expired_seconds, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    const std::string& lua_scripts = format_string(
            "local n; n=redis.call('hsetnx','%s','%s','%s'); if (n>0) then redis.call('expire', '%s', '%u') end; return n;",
            key.c_str(), field.c_str(), value.c_str(), key.c_str(), expired_seconds);
    const RedisReplyHelper redis_reply = eval(false, key, lua_scripts, which, retry_times, force_retry);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// Time complexity: O(1)
// HGET key field
bool CRedisClient::hget(const std::string& key, const std::string& field, std::string* value, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HGET");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // Bulk string reply:
    // the value associated with field, or nil when field is not present in the hash or key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return false;
    if (REDIS_REPLY_STRING == redis_reply->type)
        return get_value(redis_reply.get(), value);
    return true;
}

// Time complexity: O(1)
// HINCRBY key field increment
int64_t CRedisClient::hincrby(const std::string& key, const std::string& field, int64_t increment, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HINCRBY");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.add_arg(increment);
    cmd_args.final();

    // Integer reply: the value at field after the increment operation.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int64_t>(redis_reply->integer);
    return 0;
}

void CRedisClient::hincrby(const std::string& key, const std::vector<std::pair<std::string, int64_t> >& increments, std::vector<int64_t>* values, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    hmincrby(key, increments, values, which, retry_times, force_retry);
}

void CRedisClient::hmincrby(const std::string& key, const std::vector<std::pair<std::string, int64_t> >& increments, std::vector<int64_t>* values, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    const std::string& lua_scripts = format_string(
            "local j=1;local results={};for i=1,#ARGV,2 do local f=ARGV[i];local v=ARGV[i+1];results[j]=redis.call('hincrby','%s',f,v);j=j+1; end;return results;",
            key.c_str());
    std::vector<std::string> parameters(2*increments.size());
    for (std::vector<std::pair<std::string, int64_t> >::size_type i=0,j=0; i<increments.size(); ++i,j+=2)
    {
        const std::pair<std::string, int64_t>& increment = increments[i];
        parameters[j] = increment.first;
        parameters[j+1] = any2string(increment.second);
    }
    const RedisReplyHelper redis_reply = eval(false, key, lua_scripts, parameters, which, retry_times, force_retry);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        get_values(redis_reply.get(), values);
}

void CRedisClient::hset(const std::string& key, const std::map<std::string, std::string>& map, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    hmset(key, map, which, retry_times, force_retry);
}

// Time complexity: O(N) where N is the number of fields being set.
// HMSET key field value [field value ...]
void CRedisClient::hmset(const std::string& key, const std::map<std::string, std::string>& map, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HMSET");
    cmd_args.add_arg(key);
    cmd_args.add_args(map);
    cmd_args.final();

    // Simple string reply (REDIS_REPLY_STATUS)
    redis_command(false, force_retry, retry_times, key, cmd_args, which);
}

int CRedisClient::hget(const std::string& key, const std::vector<std::string>& fields, std::map<std::string, std::string>* map, bool keep_null, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    return hmget(key, fields, map, keep_null, which, retry_times);
}

// Time complexity: O(N) where N is the number of fields being requested.
// HMGET key field [field ...]
int CRedisClient::hmget(const std::string& key, const std::vector<std::string>& fields, std::map<std::string, std::string>* map, bool keep_null, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HMGET");
    cmd_args.add_arg(key);
    cmd_args.add_args(fields);
    cmd_args.final();

    // Array reply:
    // list of values associated with the given fields, in the same order as they are requested.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return 0;
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), fields, keep_null, map);
    return 0;
}

// Time complexity: O(N) where N is the size of the hash.
// HGETALL key
int CRedisClient::hgetall(const std::string& key, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HGETALL");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Array reply:
    // list of fields and their values stored in the hash, or an empty list when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return 0;
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), map);
    return 0;
}

// Time complexity: O(1)
// HSTRLEN key field
int CRedisClient::hstrlen(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HSTRLEN");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // Integer reply:
    // the string length of the value associated with field,
    // or zero when field is not present in the hash or key does not exist at all.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return 0;
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(N) where N is the size of the hash.
// HKEYS key
int CRedisClient::hkeys(const std::string& key, std::vector<std::string>* fields, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HKEYS");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Array reply:
    // list of fields in the hash, or an empty list when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return 0;
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), fields);
    return 0;
}

// Time complexity: O(N) where N is the size of the hash.
// HVALS key
int CRedisClient::hvals(const std::string& key, std::vector<std::string>* vals, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HVALS");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Array reply:
    // list of values in the hash, or an empty list when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return 0;
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), vals);
    return 0;
}

// Time complexity:
// O(1) for every call. O(N) for a complete iteration,
// including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection..
//
// HSCAN key cursor [MATCH pattern] [COUNT count]
int64_t CRedisClient::hscan(const std::string& key, int64_t cursor, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    return hscan(key, cursor, std::string(""), 0, map, which, retry_times);
}

int64_t CRedisClient::hscan(const std::string& key, int64_t cursor, int count, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    return hscan(key, cursor, std::string(""), count, map, which, retry_times);
}

int64_t CRedisClient::hscan(const std::string& key, int64_t cursor, const std::string& pattern, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    return hscan(key, cursor, pattern, 0, map, which, retry_times);
}

int64_t CRedisClient::hscan(const std::string& key, int64_t cursor, const std::string& pattern, int count, std::map<std::string, std::string>* map, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("HSCAN");
    cmd_args.add_arg(key);
    cmd_args.add_arg(cursor);
    if (!pattern.empty())
    {
        cmd_args.add_arg("MATCH");
        cmd_args.add_arg(pattern);
    }
    if (count > 0)
    {
        cmd_args.add_arg("COUNT");
        cmd_args.add_arg(count);
    }
    cmd_args.final();

    // (gdb) p *redis_reply
    // $1 = {type = 2, integer = 0, len = 0, str = 0x0, elements = 2, element = 0x641450}
    // (gdb) p *redis_reply->element[0]
    // $2 = {type = 1, integer = 0, len = 1, str = 0x641820 "0", elements = 0, element = 0x0}
    // (gdb) p *redis_reply->element[1]
    // $3 = {type = 2, integer = 0, len = 0, str = 0x0, elements = 8, element = 0x6414b0}
    // (gdb) p *redis_reply->element[1]->element[0]
    // $4 = {type = 1, integer = 0, len = 2, str = 0x6418f0 "f1", elements = 0, element = 0x0}
    // (gdb) p *redis_reply->element[1]->element[1]
    // $5 = {type = 1, integer = 0, len = 2, str = 0x641950 "v1", elements = 0, element = 0x0}
    // (gdb) p *redis_reply->element[1]->element[2]
    // $6 = {type = 1, integer = 0, len = 2, str = 0x6419b0 "f2", elements = 0, element = 0x0}
    // (gdb) p *redis_reply->element[1]->element[3]
    // $7 = {type = 1, integer = 0, len = 2, str = 0x641a10 "v2", elements = 0, element = 0x0}
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
    {
        get_values(redis_reply->element[1], map);
        return static_cast<int64_t>(atoll(redis_reply->element[0]->str));
    }
    return 0;
}

//
// LIST
//

// Time complexity: O(1)
// LLEN key
int CRedisClient::llen(const std::string& key, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("LLEN");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply: the length of the list at key.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
// LPOP key
bool CRedisClient::lpop(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("LPOP");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Bulk string reply: the value of the first element, or nil when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return false;
    if (REDIS_REPLY_STRING == redis_reply->type)
        return get_value(redis_reply.get(), value);
    return true; // MULTI & EXEC the type always is REDIS_REPLY_STATUS
}

// Time complexity: O(1)
// LPUSH key value [value ...]
int CRedisClient::lpush(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("LPUSH");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply: the length of the list after the push operations.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
// LPUSH key value [value ...]
int CRedisClient::lpush(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("LPUSH");
    cmd_args.add_arg(key);
    cmd_args.add_args(values);
    cmd_args.final();

    // Integer reply: the length of the list after the push operations.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity:
// O(S+N) where S is the distance of start offset from HEAD for small lists,
// from nearest end (HEAD or TAIL) for large lists;
// and N is the number of elements in the specified range.
//
// LRANGE key start stop
int CRedisClient::lrange(const std::string& key, int64_t start, int64_t end, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("LRANGE");
    cmd_args.add_arg(key);
    cmd_args.add_arg(start);
    cmd_args.add_arg(end);
    cmd_args.final();

    // Array reply: list of elements in the specified range.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), values);
    return 0;
}

// Time complexity:
// O(N) where N is the number of elements to be removed by the operation.
//
// LTRIM key start stop
void CRedisClient::ltrim(const std::string& key, int64_t start, int64_t end, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("LTRIM");
    cmd_args.add_arg(key);
    cmd_args.add_arg(start);
    cmd_args.add_arg(end);
    cmd_args.final();

    // Simple string reply (REDIS_REPLY_STATUS)
    redis_command(false, force_retry, retry_times, key, cmd_args, which);
}

// Time complexity: O(1)
// RPOP key
bool CRedisClient::rpop(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("RPOP");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Bulk string reply: the value of the last element, or nil when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return false;
    if (REDIS_REPLY_STRING == redis_reply->type)
        return get_value(redis_reply.get(), value);
    return true;
}

// Time complexity: O(1)
int CRedisClient::rpush(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("RPUSH");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply: the length of the list after the push operation.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
int CRedisClient::rpush(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("RPUSH");
    cmd_args.add_arg(key);
    cmd_args.add_args(values);
    cmd_args.final();

    // Integer reply: the length of the list after the push operation.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
int CRedisClient::rpushx(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("RPUSHX");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply:
    // the length of the list after the push operation.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

//
// SET
//

// Time complexity:
//  O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
int CRedisClient::sadd(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SADD");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply: the number of elements that were added to the set,
    // not including all the elements already present into the set.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

//  O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
int CRedisClient::sadd(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SADD");
    cmd_args.add_arg(key);
    cmd_args.add_args(values);
    cmd_args.final();

    // Integer reply: the number of elements that were added to the set,
    // not including all the elements already present into the set.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// O(1)
int CRedisClient::scard(const std::string& key, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SCARD");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply: the cardinality (number of elements) of the set, or 0 if key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// O(1)
bool CRedisClient::sismember(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SISMEMBER");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if the element is a member of the set.
    // 0 if the element is not a member of the set, or if key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// O(N) where N is the set cardinality.
int CRedisClient::smembers(const std::string& key, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SMEMBERS");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Array reply:
    // all elements of the set.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), values);
    return 0;
}

// Time complexity: O(1)
bool CRedisClient::spop(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    std::vector<std::string> values;
    const int n = spop(key, 1, &values, which, retry_times, force_retry);
    if (n > 0)
    {
        *value = value[0];
    }
    return n > 0;
}

// Time complexity: O(1)
int CRedisClient::spop(const std::string& key, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SPOP");
    cmd_args.add_arg(key);
    cmd_args.add_arg(count);
    cmd_args.final();

    // Bulk string reply:
    // the removed element, or nil when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), values);
    return 0;
}

// Time complexity: O(1)
bool CRedisClient::srandmember(const std::string& key, std::string* value, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SRANDMEMBER");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Bulk string reply:
    // without the additional count argument the command returns a Bulk Reply with the randomly selected element,
    // or nil when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return false;
    if (REDIS_REPLY_STRING == redis_reply->type)
        return get_value(redis_reply.get(), value);
    return true;
}

// Time complexity: O(N) where N is the absolute value of the passed count.
int CRedisClient::srandmember(const std::string& key, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SRANDMEMBER");
    cmd_args.add_arg(key);
    cmd_args.add_arg(count);
    cmd_args.final();

    // Array reply:
    // when the additional count argument is passed the command returns an array of elements,
    // or an empty array when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), values);
    return 0;
}

// Time complexity: O(N) where N is the number of members to be removed.
int CRedisClient::srem(const std::string& key, const std::string& value, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SREM");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply: the number of members that were removed from the set,
    // not including non existing members.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

int CRedisClient::srem(const std::string& key, const std::vector<std::string>& values, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("SREM");
    cmd_args.add_arg(key);
    cmd_args.add_args(values);
    cmd_args.final();

    // Integer reply: the number of members that were removed from the set,
    // not including non existing members.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity:
// O(1) for every call.
// O(N) for a complete iteration, including enough command calls for the cursor to return back to 0.
// N is the number of elements inside the collection..
int64_t CRedisClient::sscan(const std::string& key, int64_t cursor, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    return sscan(key, cursor, std::string(""), 0, values, which, retry_times);
}

int64_t CRedisClient::sscan(const std::string& key, int64_t cursor, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    return sscan(key, cursor, std::string(""), count, values, which, retry_times);
}

int64_t CRedisClient::sscan(const std::string& key, int64_t cursor, const std::string& pattern, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    return sscan(key, cursor, pattern, 0, values, which, retry_times);
}

int64_t CRedisClient::sscan(const std::string& key, int64_t cursor, const std::string& pattern, int count, std::vector<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    // SSCAN key cursor [MATCH pattern] [COUNT count]
    CCommandArgs cmd_args;
    cmd_args.add_arg("SSCAN");
    cmd_args.add_arg(key);
    cmd_args.add_arg(cursor);
    if (!pattern.empty())
    {
        cmd_args.add_arg("MATCH");
        cmd_args.add_arg(pattern);
    }
    if (count > 0)
    {
        cmd_args.add_arg("COUNT");
        cmd_args.add_arg(count);
    }
    cmd_args.final();

    // (gdb) p *redis_reply
    // $3 = {type = 2, integer = 0, len = 0, str = 0x0, elements = 2, element = 0x641450}
    // (gdb) p *redis_reply->element[0]
    // $4 = {type = 1, integer = 0, len = 4, str = 0x641810 "6656", elements = 0, element = 0x0}
    // (gdb) p *redis_reply->element[1]
    // $5 = {type = 2, integer = 0, len = 0, str = 0x0, elements = 10, element = 0x641930}
    // (gdb) p *redis_reply->element[1]->element[0]
    // $6 = {type = 1, integer = 0, len = 5, str = 0x6419d0 "f1055", elements = 0, element = 0x0}
    // (gdb) p *redis_reply->element[1]->element[1]
    // $7 = {type = 1, integer = 0, len = 5, str = 0x641a30 "f6368", elements = 0, element = 0x0}
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
    {
        get_values(redis_reply->element[1], values);
        return static_cast<int64_t>(atoll(redis_reply->element[0]->str));
    }
    return 0;
}

int64_t CRedisClient::sscan(const std::string& key, int64_t cursor, const std::string& pattern, int count, std::set<std::string>* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    // SSCAN key cursor [MATCH pattern] [COUNT count]
    CCommandArgs cmd_args;
    cmd_args.add_arg("SSCAN");
    cmd_args.add_arg(key);
    cmd_args.add_arg(cursor);
    if (!pattern.empty())
    {
        cmd_args.add_arg("MATCH");
        cmd_args.add_arg(pattern);
    }
    if (count > 0)
    {
        cmd_args.add_arg("COUNT");
        cmd_args.add_arg(count);
    }
    cmd_args.final();

    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
    {
        get_values(redis_reply->element[1], values);
        return static_cast<int64_t>(atoll(redis_reply->element[0]->str));
    }
    return 0;
}

//
// ZSET
//

// Time complexity:
// O(M*log(N)) with N being the number of elements in the sorted set and M the number of elements to be removed.
//
// ZREM key member [member ...]
int CRedisClient::zrem(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZREM");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // Integer reply, specifically:
    // The number of members removed from the sorted set, not including non existing members.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity:
// O(M*log(N)) with N being the number of elements in the sorted set and M the number of elements to be removed.
int CRedisClient::zrem(const std::string& key, const std::vector<std::string>& fields, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZREM");
    cmd_args.add_arg(key);
    cmd_args.add_args(fields);
    cmd_args.final();

    // Integer reply, specifically:
    // The number of members removed from the sorted set, not including non existing members.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity:
// O(log(N)) for each item added, where N is the number of elements in the sorted set.
//
// ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
int CRedisClient::zadd(const std::string& key, const std::string& field, int64_t score, ZADDFLAG flag, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    std::map<std::string, int64_t> map;
    map[field] = score;
    return zadd(key, map, flag, which, retry_times, force_retry);
}

int CRedisClient::zadd(const std::string& key, const std::map<std::string, int64_t>& map, ZADDFLAG flag, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    const std::string& flag_str = zaddflag2str(flag);
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZADD");
    cmd_args.add_arg(key);
    if (!flag_str.empty())
    {
        cmd_args.add_arg(flag_str);
    }
    cmd_args.add_args(map, true);
    cmd_args.final();

    // Integer reply, specifically:
    // The number of elements added to the sorted sets, not including
    // elements already existing for which the score was updated.
    //
    // If the INCR option is specified, the return value will be Bulk string reply:
    // the new score of member (a double precision floating point number), represented as string.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
int64_t CRedisClient::zcard(const std::string& key, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZCARD");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply:
    // the cardinality (number of elements) of the sorted set, or 0 if key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int64_t>(redis_reply->integer);
    return 0;
}

// Time complexity:
// O(log(N)) with N being the number of elements in the sorted set.
//
// ZCOUNT key min max
int64_t CRedisClient::zcount(const std::string& key, int64_t min, int64_t max , std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZCOUNT");
    cmd_args.add_arg(key);
    cmd_args.add_arg(min);
    cmd_args.add_arg(max);
    cmd_args.final();

    // Integer reply:
    // the number of elements in the specified score range.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int64_t>(redis_reply->integer);
    return 0;
}

// Time complexity:
// O(log(N)) where N is the number of elements in the sorted set.
//
// ZINCRBY key increment member
int64_t CRedisClient::zincrby(const std::string& key, const std::string& field, int64_t increment, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZINCRBY");
    cmd_args.add_arg(key);
    cmd_args.add_arg(increment);
    cmd_args.add_arg(field);
    cmd_args.final();

    // Bulk string reply:
    // the new score of member (a double precision floating point number), represented as string.
    //
    // (gdb) p *redis_reply._redis_reply
    // $4 = {type = 1, integer = 0, len = 2, str = 0x647860 "10", elements = 0, element = 0x0}
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_STRING == redis_reply->type)
        return static_cast<int64_t>(atoll(redis_reply->str));
    return 0;
}

// Time complexity:
// O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned.
// ZRANGE key start stop [WITHSCORES]
//
// ZRANGE key start stop [WITHSCORES]
int CRedisClient::zrange(const std::string& key, int64_t start, int64_t end, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZRANGE");
    cmd_args.add_arg(key);
    cmd_args.add_arg(start);
    cmd_args.add_arg(end);
    if (withscores)
    {
        cmd_args.add_arg("WITHSCORES");
    }
    cmd_args.final();

    // Array reply:
    // list of elements in the specified range (optionally with their scores, in case the WITHSCORES option is given).
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), vec, withscores);
    return 0;
}

// Time complexity:
// O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned.
//
// ZREVRANGE key start stop [WITHSCORES]
int CRedisClient::zrevrange(const std::string& key, int64_t start, int64_t end, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZREVRANGE");
    cmd_args.add_arg(key);
    cmd_args.add_arg(start);
    cmd_args.add_arg(end);
    if (withscores)
    {
        cmd_args.add_arg("WITHSCORES");
    }
    cmd_args.final();

    // Array reply:
    // list of elements in the specified range (optionally with their scores, in case the WITHSCORES option is given).
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), vec, withscores);
    return 0;
}

// Time complexity:
// O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned.
// If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
//
// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
int CRedisClient::zrangebyscore(const std::string& key, int64_t min, int64_t max, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZRANGEBYSCORE");
    cmd_args.add_arg(key);
    cmd_args.add_arg(min);
    cmd_args.add_arg(max);
    if (withscores)
    {
        cmd_args.add_arg("WITHSCORES");
    }
    cmd_args.final();

    // Array reply:
    // list of elements in the specified score range (optionally with their scores).
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), vec, withscores);
    return 0;
}

// Time complexity:
// O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned.
// If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
//
// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
int CRedisClient::zrevrangebyscore(const std::string& key, int64_t max, int64_t min, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZREVRANGEBYSCORE");
    cmd_args.add_arg(key);
    cmd_args.add_arg(max);
    cmd_args.add_arg(min);
    if (withscores)
    {
        cmd_args.add_arg("WITHSCORES");
    }
    cmd_args.final();

    // Array reply:
    // list of elements in the specified score range (optionally with their scores).
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), vec, withscores);
    return 0;
}

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
int CRedisClient::zrangebyscore(const std::string& key, int64_t min, int64_t max, int64_t offset, int64_t count, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZREVRANGEBYSCORE");
    cmd_args.add_arg(key);
    cmd_args.add_arg(min);
    cmd_args.add_arg(max);
    if (withscores)
    {
        cmd_args.add_arg("WITHSCORES");
    }
    cmd_args.add_arg("LIMIT");
    cmd_args.add_arg(offset);
    cmd_args.add_arg(count);
    cmd_args.final();

    // Array reply:
    // list of elements in the specified score range (optionally with their scores).
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), vec, withscores);
    return 0;
}

// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
int CRedisClient::zrevrangebyscore(const std::string& key, int64_t max, int64_t min, int64_t offset, int64_t count, bool withscores, std::vector<std::pair<std::string, int64_t> >* vec, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZREVRANGEBYSCORE");
    cmd_args.add_arg(key);
    cmd_args.add_arg(max);
    cmd_args.add_arg(min);
    if (withscores)
    {
        cmd_args.add_arg("WITHSCORES");
    }
    cmd_args.add_arg("LIMIT");
    cmd_args.add_arg(offset);
    cmd_args.add_arg(count);
    cmd_args.final();

    // Array reply:
    // list of elements in the specified score range (optionally with their scores).
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), vec, withscores);
    return 0;
}

// Time complexity:
// O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements removed by the operation.
//
// ZREMRANGEBYRANK key start stop
int CRedisClient::zremrangebyrank(const std::string& key, int64_t start, int64_t end, std::pair<std::string, uint16_t>* which, int retry_times, bool force_retry) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZREMRANGEBYRANK");
    cmd_args.add_arg(key);
    cmd_args.add_arg(start);
    cmd_args.add_arg(end);
    cmd_args.final();

    // Integer reply:
    // the number of elements removed.
    const RedisReplyHelper redis_reply = redis_command(false, force_retry, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(log(N))
//
// ZRANK key member
int CRedisClient::zrank(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZRANK");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // If member exists in the sorted set, Integer reply: the rank of member.
    // If member does not exist in the sorted set or key does not exist, Bulk string reply: nil.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
    {
        return -1;
    }
    else
    {
        if (REDIS_REPLY_INTEGER == redis_reply->type)
            return static_cast<int>(redis_reply->integer);
        return 0;
    }
}

int CRedisClient::zrevrank(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZREVRANK");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // If member exists in the sorted set, Integer reply: the rank of member.
    // If member does not exist in the sorted set or key does not exist, Bulk string reply: nil.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
    {
        return -1;
    }
    else
    {
        if (REDIS_REPLY_INTEGER == redis_reply->type)
            return static_cast<int>(redis_reply->integer);
        return 0;
    }
}

// Time complexity: O(1)
// ZSCORE key member
int64_t CRedisClient::zscore(const std::string& key, const std::string& field, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZSCORE");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // Bulk string reply:
    // the score of member (a double precision floating point number), represented as string.
    // If member does not exist in the sorted set, or key does not exist, nil is returned.
    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
    {
        return -1;
    }
    else
    {
        if (REDIS_REPLY_STRING == redis_reply->type)
            return static_cast<int64_t>(atoll(redis_reply->str));
        return 0;
    }
}

// Time complexity:
// O(1) for every call. O(N) for a complete iteration,
// including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection..
//
// ZSCAN key cursor [MATCH pattern] [COUNT count]
int64_t CRedisClient::zscan(const std::string& key, int64_t cursor, std::vector<std::pair<std::string, int64_t> >* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    return zscan(key, cursor, std::string(""), 0, values, which, retry_times);
}

int64_t CRedisClient::zscan(const std::string& key, int64_t cursor, int count, std::vector<std::pair<std::string, int64_t> >* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    return zscan(key, cursor, std::string(""), count, values, which, retry_times);
}

int64_t CRedisClient::zscan(const std::string& key, int64_t cursor, const std::string& pattern, std::vector<std::pair<std::string, int64_t> >* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    return zscan(key, cursor, pattern, 0, values, which, retry_times);
}

int64_t CRedisClient::zscan(const std::string& key, int64_t cursor, const std::string& pattern, int count, std::vector<std::pair<std::string, int64_t> >* values, std::pair<std::string, uint16_t>* which, int retry_times) throw (CRedisException)
{
    CCommandArgs cmd_args;
    cmd_args.add_arg("ZSCAN");
    cmd_args.add_arg(key);
    cmd_args.add_arg(cursor);
    if (!pattern.empty())
    {
        cmd_args.add_arg("MATCH");
        cmd_args.add_arg(pattern);
    }
    if (count > 0)
    {
        cmd_args.add_arg("COUNT");
        cmd_args.add_arg(count);
    }
    cmd_args.final();

    const RedisReplyHelper redis_reply = redis_command(true, true, retry_times, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
    {
        get_values(redis_reply->element[1], values, true);
        return static_cast<int64_t>(atoll(redis_reply->element[0]->str));
    }
    return 0;
}

const RedisReplyHelper CRedisClient::redis_command(bool is_read_command, bool force_retry, int retry_times, const std::string& key, const CCommandArgs& command_args, std::pair<std::string, uint16_t>* which)
{
    //(*g_debug_log)("[%s:%d] COMMAND: %s\n", __FILE__, __LINE__, command_args.get_command());

    const int slot = cluster_mode()? get_key_slot(&key): 0;
    struct RedisNode* redis_node = NULL;
    RedisReplyHelper redis_reply;
    struct ErrorInfo errinfo;
    int retry_times_ = retry_times;
    bool to_retry;

    if (key.empty() && cluster_mode())
    {
        // 集群模式必须指定key
        errinfo.errcode = ERROR_ZERO_KEY;
        errinfo.raw_errmsg = "key not set in cluster mode";
        errinfo.errmsg = format_string("[%s:%d] %s", __FILE__, __LINE__, errinfo.raw_errmsg.c_str());
        THROW_REDIS_EXCEPTION(errinfo);
    }
    for (int rt=0; (rt<retry_times_+1)||(-1==retry_times_)||to_retry; ++rt)
    {
        bool is_node_of_slot;

        to_retry = false;
        redis_node = get_redis_node(slot, is_read_command, &is_node_of_slot, &errinfo);
        redisContext* redis_context = redis_node->context;

        if (which != NULL)
        {
            *which = redis_node->ip_and_port;
        }
        if (NULL == redis_context)
        {
            reset_slots_info(slot);
            if (_retry_sleep_milliseconds > 0)
                millisleep(_retry_sleep_milliseconds);
            continue; // RETRY
        }

        redis_reply = (redisReply*)redisCommandArgv(redis_context, command_args.get_argc(), command_args.get_argv(), command_args.get_argvlen());
        if (redis_reply)
        {
            debug_reply(command_args.get_command(), command_args.get_key(), slot, redis_reply.get(), redis_node->ip_and_port);

            if (redis_reply->type != REDIS_REPLY_ERROR)
            {
                // SUCCESS
                if (!is_read_command)
                {
                    if (redis_node->is_slave())
                    {
                        // 写操作成功了，则表示该redis_node实际是master
                        redis_node->become_master();
                    }
                    if (!is_node_of_slot)
                    {
                        // 实际正是slot对应的redis_node
                        update_slot_info(slot, redis_node->ip_and_port);
                    }
                }

                break;
            }
            else
            {
                // MOVED 6474 127.0.0.1:6380
                // WRONGTYPE Operation against a key holding the wrong kind of value
                // CLUSTERDOWN The cluster is down
                extract_errtype(redis_reply.get(), &errinfo.errtype);

                errinfo.errcode = ERROR_COMMAND;
                errinfo.raw_errmsg = redis_reply->str;
                errinfo.errmsg = format_string("[%s:%d][COMMAND:%s] (RETRY:%d/%d)%s", __FILE__, __LINE__, command_args.get_command(), rt, retry_times_, errinfo.raw_errmsg.c_str());
                (*g_error_log)("%s\n", errinfo.errmsg.c_str());

                if (is_moved_error(errinfo.errtype))
                {
                    // MOVED unconditionally RETRY
                    to_retry = true;

                    std::pair<std::string, uint16_t> node;
                    parse_moved_string(redis_reply->str, &node);
                    update_slot_info(slot, node);
                    redis_reply.free();

                    // slot对应的redis_node可能发生了主备切换
                    //
                    // 如果读策略为备不提供读服务，则在redisCommandArgv前，
                    // redis_node由master状态转为slave状态，这时redisCommandArgv也返回MOVED
                    if (is_node_of_slot)
                    {
                        // redis_node become slave from master
                        close_redis_node(redis_node);
                    }
                    continue;
                }
                else if (is_clusterdown_error(errinfo.errtype))
                {
                    redis_reply.free();
                    if (_retry_sleep_milliseconds > 0)
                        millisleep(_retry_sleep_milliseconds);
                    continue;
                }
                else
                {
                    // NOT RETRY
                    redis_reply.free();
                    break;
                }
            }
        }
        else
        {
            // REDIS_ERR_EOF (call read() return 0):
            // redis_context->err(3)
            // redis_context->errstr("Server closed the connection")
            //
            // REDIS_ERR_IO (call read() return -1):
            // redis_context->err(1)
            // redis_context->errstr("Bad file descriptor")
            //
            // REDIS_ERR_IO means there was an I/O error
            // and you should use the "errno" variable to find out what is wrong.
            // For other values, the "errstr" field will hold a description.
            errinfo.errcode = errno;
            const int redis_errcode = redis_context->err;
            const std::string& redis_errmsg = redis_context->errstr;

            // Close to reconnect
            close_redis_node(redis_node);
            // Hiredis no reset error of context method,
            // if not reset redis_context->err,
            // will always return early when the context has seen an error.
            //
            // if (c->err)
            //   return REDIS_ERR;

            // REDIS_ERR_IO: 1
            // REDIS_ERR_EOF: 3
            if ((redis_errcode != REDIS_ERR_IO) &&
                (redis_errcode != REDIS_ERR_EOF))
            {
                // NOT RETRY

                errinfo.errcode = ERROR_COMMAND;
                errinfo.raw_errmsg = format_string("(%d)%s", redis_errcode, redis_errmsg.c_str());
                errinfo.errmsg = format_string("[%s:%d][COMMAND:%s] (RETRY:%d/%d)%s", __FILE__, __LINE__, command_args.get_command(), rt, retry_times_, errinfo.raw_errmsg.c_str());
                (*g_error_log)("%s\n", errinfo.errmsg.c_str());
                break;
            }
            else
            {
                // RETRY

                // errno:
                // EINTR(4)
                // EAGAIN(11)
                // EWOULDBLOCK(11)
                // EINPROGRESS(115)
                // ECONNRESET(104)

                // errcode: EINPROGRESS(115)
                // redis_errcode: REDIS_ERR_EOF(3)
                // Server closed the connection
                //
                // errcode: ECONNRESET(104)
                // redis_errcode: REDIS_ERR_IO(1)
                // Connection reset by peer
                //
                // errcode: EAGAIN(11)
                // redis_errcode: REDIS_ERR_IO(1)
                // Resource temporarily unavailable

                // 如果同时大量主备切换，
                // 可导致所有重试均失败（errno为EAGAIN或EWOULDBLOCK），
                // 解决办法：增加重试次数或延长重试间隔时长。
                errinfo.raw_errmsg = format_string("(errcode:%d/%d)%s", errinfo.errcode, redis_errcode, redis_errmsg.c_str());
                errinfo.errmsg = format_string("[%s:%d][COMMAND:%s] (RETRY:%d/%d)%s", __FILE__, __LINE__, command_args.get_command(), rt, retry_times_, errinfo.raw_errmsg.c_str());
                (*g_error_log)("%s\n", errinfo.errmsg.c_str());

                if (REDIS_ERR_IO == redis_errcode)
                {
                    if ((EAGAIN == errinfo.errcode)
                     || (EWOULDBLOCK == errinfo.errcode))
                    {
                        // EAGAIN时，可能已经发生了主备切换，
                        // 因此这种情况下最好重置slot；如果不重置，该连接永远发现不了已切换。
                        reset_slots_info(slot);

                        // 写命令，如果遇到EAGAIN时，写命令可能已成功的，也可能未成功，对于incryby类命令应当由业务决定是否重试
                        if (!is_read_command && !force_retry)
                        {
                            break;
                        }
                    }
                }

                if (_retry_sleep_milliseconds > 0)
                    millisleep(_retry_sleep_milliseconds);
                continue;
            }
        }
    }
    if (!redis_reply)
    {
        THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(errinfo, redis_node->ip_and_port.first.c_str(), redis_node->ip_and_port.second, command_args.get_command_str(), command_args.get_key_str());
    }

    return redis_reply;
}

void CRedisClient::init()
{
    try
    {
        const int num_nodes = parse_nodes(&_nodes, _nodes_string);

        if (0 == num_nodes)
        {
            struct ErrorInfo errinfo;
            errinfo.errcode = ERROR_PARAMETER;
            errinfo.errmsg = format_string("[%s:%d] parameter[nodes] error: %s", __FILE__, __LINE__, _nodes_string.c_str());
            errinfo.raw_errmsg = format_string("parameter[nodes] error: %s", _nodes_string.c_str());
            THROW_REDIS_EXCEPTION(errinfo);
        }
        else if (1 == num_nodes)
        {
            init_standlone();
        }
        else
        {
            init_cluster();
        }
    }
    catch (...)
    {
        free_slots_info();
        free_redis_nodes();
        throw;
    }
}

void CRedisClient::init_standlone()
{
    if (_slots_info.empty())
    {
        const std::pair<std::string, uint16_t>& node = _nodes[0];
        struct ErrorInfo errinfo;

        redisContext* redis_context = connect_redis_node(-1, node, &errinfo);
        if (NULL == redis_context)
        {
            THROW_REDIS_EXCEPTION_WITH_NODE(errinfo, node.first, node.second);
        }
        else
        {
            struct SlotInfo* slot_info = new struct SlotInfo(-1);
            struct RedisNode* redis_node = add_redis_node(node, redis_context);
            slot_info->master_node = redis_node;
            _slots_info.push_back(slot_info);
        }
    }
}

void CRedisClient::init_cluster()
{
    struct ErrorInfo errinfo;

    _slots_info.resize(CLUSTER_SLOTS);
    for (int i=0; i<CLUSTER_SLOTS; ++i)
    {
        struct SlotInfo* slot_info = new struct SlotInfo(i);
        _slots_info[i] = slot_info;
    }

    for (int i=0; i<static_cast<int>(_nodes.size()); ++i)
    {
        const std::pair<std::string, uint16_t>& node = _nodes[i];

        redisContext* redis_context = connect_redis_node(-1, node, &errinfo);
        if (redis_context != NULL)
        {
            // _nodes[A] may be the same as _nodes[B]
            add_redis_node(node, redis_context);
        }
        else
        {
            (*g_debug_log)("[%s:%d] (%d)%s\n", __FILE__, __LINE__, errinfo.errcode, errinfo.errmsg.c_str());
        }
    }
    if (_redis_contexts.empty())
    {
        THROW_REDIS_EXCEPTION(errinfo);
    }
}

void CRedisClient::free_slots_info()
{
    const int num_slots = static_cast<int>(_slots_info.size());
    for (int i=0; i<num_slots; ++i)
    {
        struct SlotInfo* slot_info = _slots_info[i];
        delete slot_info;
    }

    _slots_info.clear();
}

void CRedisClient::reset_slots_info(int slot)
{
    struct SlotInfo* slot_info = _slots_info[slot];
    if (slot_info != NULL)
        slot_info->master_node = NULL;
}

void CRedisClient::update_slot_info(int slot, const std::pair<std::string, uint16_t>& node)
{
    struct RedisNode* redis_node;
    struct ErrorInfo errinfo;

    struct SlotInfo* slot_info = _slots_info[slot];
    if (slot_info->master_node != NULL)
    {
        redis_node = slot_info->master_node;
    }
    else
    {
        redis_node = find_redis_node(node);
        if (NULL == redis_node)
        {
            redis_node = add_redis_node(node, NULL);
        }

        slot_info->master_node = redis_node;
    }

    // update_slot_info更新的一定是master状态的redis_node，
    // 如果redis_node为slave状态，则需要先断开原有的redis连接，再重新建立连接，以切换为master状态。
    if (redis_node->is_slave())
    {
        close_redis_node(redis_node); // redis_node->context will be NULL
    }

    redis_node->ip_and_port = node;
    if (NULL == redis_node->context)
    {
        redis_node->context = connect_redis_node(slot, node, &errinfo);

        if (NULL == redis_node->context)
        {
            redis_node->become_slave();
            (*g_error_log)("[%s:%d] update slot(%d) with %s:%d failed: (%d)%s\n", __FILE__, __LINE__, slot, node.first.c_str(), node.second, errinfo.errcode, errinfo.errmsg.c_str());
        }
        else
        {
            redis_node->become_master();
            (*g_info_log)("[%s:%d] update slot(%d) with %s:%d ok\n", __FILE__, __LINE__, slot, node.first.c_str(), node.second);
        }
    }
}

void CRedisClient::free_redis_nodes()
{
    for (std::map<std::pair<std::string, uint16_t>, struct RedisNode*>::iterator iter=_redis_contexts.begin(); iter!=_redis_contexts.end(); ++iter)
    {
        struct RedisNode* redis_node = iter->second;
        close_redis_node(redis_node);
        delete redis_node;
    }
    _redis_contexts.clear();
}

redisContext* CRedisClient::connect_redis_node(int slot, const std::pair<std::string, uint16_t>& node, struct ErrorInfo* errinfo) const
{
    redisContext* redis_context = NULL;

    errinfo->clear();
    if (_connect_timeout_milliseconds <= 0)
    {
        redis_context = redisConnect(node.first.c_str(), node.second);
    }
    else
    {
        struct timeval timeout;
        timeout.tv_sec = _connect_timeout_milliseconds / 1000;
        timeout.tv_usec = (_connect_timeout_milliseconds % 1000) * 1000;
        redis_context = redisConnectWithTimeout(node.first.c_str(), node.second, timeout);
    }

    if (NULL == redis_context)
    {
        // can't allocate redis context
        errinfo->errcode = ERROR_REDIS_CONTEXT;
        errinfo->raw_errmsg = "can't allocate redis context";
        errinfo->errmsg = format_string("[%s:%d][%s:%d][SLOT:%d] %s", __FILE__, __LINE__, node.first.c_str(), node.second, slot, errinfo->raw_errmsg.c_str());
        (*g_error_log)("%s\n", errinfo->errmsg.c_str());
    }
    else if (redis_context->err != 0)
    {
        // #define REDIS_ERR_IO 1 /* Error in read or write */
        // redis_context->errstr
        //
        // Connection refused
        //
        // errno: EADDRNOTAVAIL(99)
        // err: REDIS_ERR_IO(1)
        // Cannot assign requested address

        errinfo->errcode = ERROR_INIT_REDIS_CONN;
        errinfo->raw_errmsg = redis_context->errstr;
        if (REDIS_ERR_IO == redis_context->err)
            errinfo->errmsg = format_string("[%s:%d][%s:%d][SLOT:%d] (errno:%d,err:%d)%s", __FILE__, __LINE__, node.first.c_str(), node.second, slot, errno, redis_context->err, errinfo->raw_errmsg.c_str());
        else
            errinfo->errmsg = format_string("[%s:%d][%s:%d][SLOT:%d] (err:%d)%s", __FILE__, __LINE__, node.first.c_str(), node.second, slot, redis_context->err, errinfo->raw_errmsg.c_str());
        (*g_error_log)("%s\n", errinfo->errmsg.c_str());
        redisFree(redis_context);
        redis_context = NULL;
    }
    else
    {
        if (_data_timeout_milliseconds > 0)
        {
            struct timeval data_timeout;
            data_timeout.tv_sec = _data_timeout_milliseconds / 1000;
            data_timeout.tv_usec = (_data_timeout_milliseconds % 1000) * 1000;

            if (REDIS_ERR == redisSetTimeout(redis_context, data_timeout))
            {
                // REDIS_ERR_IO == redis_context->err
                errinfo->errcode = ERROR_INIT_REDIS_CONN;
                errinfo->raw_errmsg = redis_context->errstr;
                errinfo->errmsg = format_string("[%s:%d][%s:%d][SLOT:%d] (errno:%d,err:%d)%s", __FILE__, __LINE__, node.first.c_str(), node.second, slot, errno, redis_context->err, errinfo->raw_errmsg.c_str());
                (*g_error_log)("%s\n", errinfo->errmsg.c_str());
                redisFree(redis_context);
                redis_context = NULL;
            }
        }

        if ((0 == errinfo->errcode) && !_password.empty())
        {
            const redisReply* redis_reply = (redisReply*)redisCommand(redis_context, "AUTH %s", _password.c_str());

            if (redis_reply != NULL)
            {
                freeReplyObject((void*)redis_reply);
            }
            else
            {
                // Authorization failed
                errinfo->errcode = ERROR_REDIS_AUTH;
                errinfo->raw_errmsg = "authorization failed";
                errinfo->errmsg = format_string("[%s:%d][%s:%d][SLOT:%d] %s", __FILE__, __LINE__, node.first.c_str(), node.second, slot, errinfo->raw_errmsg.c_str());
                (*g_error_log)("%s\n", errinfo->errmsg.c_str());
                redisFree(redis_context);
                redis_context = NULL;
            }
        }
    }

    return redis_context;
}

void CRedisClient::close_redis_node(struct RedisNode*& redis_node)
{
    if (redis_node != NULL)
    {
        redis_node->become_slave();
        redisFree(redis_node->context);
        redis_node->context = NULL;
    }
}

struct RedisNode* CRedisClient::find_redis_node(const std::pair<std::string, uint16_t>& node)
{
    struct RedisNode* redis_node = NULL;
    std::map<std::pair<std::string, uint16_t>, struct RedisNode*>::iterator iter =
            _redis_contexts.find(node);
    if (iter != _redis_contexts.end())
    {
        redis_node = iter->second;
    }
    return redis_node;
}

struct RedisNode* CRedisClient::get_redis_node(int slot, bool is_read_command, bool* is_node_of_slot, struct ErrorInfo* errinfo)
{
    struct RedisNode* redis_node = NULL;
    struct SlotInfo* slot_info = _slots_info[slot];

    if (NULL == slot_info->master_node)
    {
        // 对应的slot还未初始化
        *is_node_of_slot = false;

        (*g_debug_log)("[%s:%d] slot(%d) without any node\n", __FILE__, __LINE__, slot);
        for (std::map<std::pair<std::string, uint16_t>, struct RedisNode*>::iterator iter=_redis_contexts.begin(); iter!=_redis_contexts.end(); ++iter)
        {
            redis_node = iter->second;

            if (redis_node->context != NULL)
            {
                break;
            }
        }

        if (NULL == redis_node->context)
        {
            (*g_debug_log)("[%s:%d] without any node\n", __FILE__, __LINE__, slot);

            for (std::map<std::pair<std::string, uint16_t>, struct RedisNode*>::iterator iter=_redis_contexts.begin(); iter!=_redis_contexts.end(); ++iter)
            {
                redis_node = iter->second;
                redis_node->context = connect_redis_node(slot, redis_node->ip_and_port, errinfo);
                if (redis_node->context != NULL)
                {
                    break;
                }
            }
        }
    }
    else
    {
        // 对应的slot已初始化过
        *is_node_of_slot = true;

        if (!is_read_command)
        {
            // Write command always chooses master node.
            redis_node = slot_info->master_node;
        }
        else if (RP_ONLEY_MASTER == _read_policy)
        {
            // Always read from master.
            redis_node = slot_info->master_node;
        }
        else
        {
            redis_node = slot_info->master_node;
        }

        if (NULL == redis_node->context)
        {
            redis_node->context = connect_redis_node(slot, redis_node->ip_and_port, errinfo);
        }
    }

    return redis_node;
}

struct RedisNode* CRedisClient::add_redis_node(const std::pair<std::string, uint16_t>& node, redisContext* redis_context)
{
    struct RedisNode* redis_node = new struct RedisNode(-1);
    redis_node->context = redis_context;
    redis_node->ip_and_port = node;

    std::pair<std::map<std::pair<std::string, uint16_t>, struct RedisNode*>::iterator, bool> ret =
            _redis_contexts.insert(std::make_pair(node, redis_node));
    if (!ret.second)
    {
        if (ret.first->second->context != NULL)
        {
            // Having the same nodes：
            // CRedisClient redis("10.49.126.98:1379,10.49.126.98:1379");
            redisFree(ret.first->second->context);
        }

        ret.first->second->context = redis_context;
        ret.first->second->ip_and_port = node;
        delete redis_node;
        redis_node = ret.first->second;
    }

    return redis_node;
}

bool CRedisClient::get_nodes_info(std::vector<struct NodeInfo>* nodes_info, struct ErrorInfo* errinfo, int i, redisContext* redis_context, const std::pair<std::string, uint16_t>& node)
{
    const RedisReplyHelper redis_reply = (redisReply*)redisCommand(redis_context, "CLUSTER NODES");

    errinfo->clear();
    if (!redis_reply)
    {
        (*g_error_log)("[i:%d][%s:%d][%s:%d] redisCommand failed\n", i, __FILE__, __LINE__, node.first.c_str(), node.second);
        errinfo->errcode = ERROR_COMMAND;
        errinfo->errmsg = format_string("[%s:%d] %s", __FILE__, __LINE__, "redisCommand failed");
        errinfo->raw_errmsg = "redisCommand failed";
    }
    else if (REDIS_REPLY_ERROR == redis_reply->type)
    {
        // ERR This instance has cluster support disabled
        (*g_error_log)("[i:%d][%s:%d][%s:%d] (err:%d)%s\n", i, __FILE__, __LINE__, node.first.c_str(), node.second, redis_context->err, redis_reply->str);
        errinfo->errcode = ERROR_COMMAND;
        errinfo->errmsg = format_string("[%s:%d] %s", __FILE__, __LINE__, redis_reply->str);
        errinfo->raw_errmsg = redis_reply->str;
    }
    else if (redis_reply->type != REDIS_REPLY_STRING)
    {
        // Unexpected reply type
        (*g_error_log)("[i:%d][%s:%d][%s:%d] Unexpected reply type: %d\n", i, __FILE__, __LINE__, node.first.c_str(), node.second, redis_reply->type);
        errinfo->errcode = ERROR_UNEXCEPTED_REPLY_TYPE;
        errinfo->errmsg = format_string("[%s:%d] (type:%d)%s", __FILE__, __LINE__, redis_reply->type, redis_reply->str);
        errinfo->raw_errmsg = redis_reply->str;
    }
    else
    {
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
        const int num_lines = split(&lines, std::string(redis_reply->str), std::string("\n"));

        if (num_lines < 1)
        {
            errinfo->errcode = ERROR_REPLY_FORMAT;
            errinfo->errmsg = format_string("[%s:%d] %s", __FILE__, __LINE__, "reply nothing");
            errinfo->raw_errmsg = "reply nothing";
        }
        for (int row=0; row<num_lines; ++row)
        {
            std::vector<std::string> tokens;
            const std::string& line = lines[row];
            const int num_tokens = split(&tokens, line, std::string(" "));

            if (0 == num_tokens)
            {
                // Over
                errinfo->clear();
                break;
            }
            else if (num_tokens < 8)
            {
                (*g_error_log)("[i:%d][%s:%d][%s:%d] The number(%d) of tokens not matched: %s\n", i, __FILE__, __LINE__, node.first.c_str(), node.second, num_tokens, line.c_str());
                nodes_info->clear();
                errinfo->errcode = ERROR_REPLY_FORMAT;
                errinfo->errmsg = format_string("[%s:%d] %s", __FILE__, __LINE__, "reply format error");
                errinfo->raw_errmsg = "reply format error";
                break;
            }
            else
            {
                NodeInfo node_info;
                node_info.id = tokens[0];

                if (!parse_node_string(tokens[1], &node_info.ip, &node_info.port))
                {
                    (*g_error_log)("[i:%d][%s:%d][%s:%d] Invalid node string: %s\n", i, __FILE__, __LINE__, node.first.c_str(), node.second, line.c_str());
                    nodes_info->clear();
                    errinfo->errcode = ERROR_REPLY_FORMAT;
                    errinfo->errmsg = format_string("[%s:%d] %s", __FILE__, __LINE__, "reply format error");
                    errinfo->raw_errmsg = "reply format error";
                    break;
                }
                else
                {
                    node_info.flags = tokens[2];
                    node_info.master_id = tokens[3];
                    node_info.ping_sent = atoi(tokens[4].c_str());
                    node_info.pong_recv = atoi(tokens[5].c_str());
                    node_info.epoch = atoi(tokens[6].c_str());
                    node_info.connected = (tokens[7] == "connected");

                    if (node_info.is_master())
                    {
                        for (int col=8; col<num_tokens; ++col)
                        {
                            std::pair<int, int> slot;
                            parse_slot_string(tokens[col], &slot.first, &slot.second);
                            node_info.slots.push_back(slot);
                        }
                    }

                    (*g_debug_log)("[i:%d][%s:%d][%s:%d] %s\n", i, __FILE__, __LINE__, node.first.c_str(), node.second, node_info.str().c_str());
                    nodes_info->push_back(node_info);
                }
            }
        }
    }

    return !nodes_info->empty();
}

bool CRedisClient::get_slave_nodes(redisContext* redis_context, std::vector<std::pair<std::string, uint16_t> >* nodes)
{
    const RedisReplyHelper redis_reply = (redisReply*)redisCommand(redis_context, "%s", "INFO Replication");

    if (!redis_reply || (redis_reply->type!=REDIS_REPLY_STRING))
    {
        return false;
    }
    else
    {
        nodes->clear();

        /*
         * # Replication
         * role:master
         * connected_slaves:1
         * slave0:ip=10.49.126.98,port=1380,state=online,offset=12309344,lag=0
         * master_repl_offset:12309344
         * repl_backlog_active:1
         * repl_backlog_size:1048576
         * repl_backlog_first_byte_offset:11260769
         * repl_backlog_histlen:1048576
         */
        std::vector<std::string> lines;
        const int num_lines = split(&lines, std::string(redis_reply->str), std::string("\n"));
        for (int row=0; row<num_lines; ++row)
        {
            const std::string& line = lines[row];

            if ((line.size() >= sizeof("slave0:ip=1.2.3.4,port=2,state=?,offset=1,lag=0")-1) &&
                (0 == strncmp(line.c_str(), "slave", sizeof("slave")-1)))
            {
                const std::string::size_type first_equal_pos = line.find('=');
                const std::string::size_type first_comma_pos = line.find(',');

                if ((first_equal_pos != std::string::npos) &&
                    (first_comma_pos != std::string::npos))
                {
                    const std::string::size_type second_equal_pos = line.find('=', first_comma_pos+1);
                    const std::string::size_type second_comma_pos = line.find(',', first_comma_pos+1);

                    if ((second_equal_pos != std::string::npos) &&
                        (second_comma_pos != std::string::npos))
                    {
                        const std::string& ip_str = line.substr(first_equal_pos+1, first_comma_pos-first_equal_pos-1);
                        const std::string& port_str = line.substr(second_equal_pos+1, second_comma_pos-second_equal_pos-1);
                        nodes->push_back(std::make_pair(ip_str, (uint16_t)atoi(port_str.c_str())));
                    }
                }
            }
        }

        return !nodes->empty();
    }
}

// Extract error type, such as ERR, MOVED, WRONGTYPE, ...
void CRedisClient::extract_errtype(const redisReply* redis_reply, std::string* errtype)
{
    if (redis_reply->len > 2)
    {
        const char* space_pos = strchr(redis_reply->str, ' ');

        if (space_pos != NULL)
        {
            const size_t len = static_cast<size_t>(space_pos - redis_reply->str);

            if (len > 2)
            {
                if (isupper(redis_reply->str[0]) &&
                    isupper(redis_reply->str[1]) &&
                    isupper(redis_reply->str[2]))
                {
                    errtype->assign(redis_reply->str, len);
                }
            }
        }
    }
}

bool CRedisClient::get_value(const redisReply* redis_reply, std::string* value)
{
    value->clear();

    if (REDIS_REPLY_NIL == redis_reply->type)
    {
        return false;
    }
    else
    {
        if (redis_reply->len > 0)
            value->assign(redis_reply->str, redis_reply->len);
        else
            value->clear();
        return true;
    }
}

int CRedisClient::get_values(const redisReply* redis_reply, std::vector<std::string>* values)
{
    values->clear();

    if (redis_reply->elements > 0)
    {
        values->resize(redis_reply->elements);
        for (size_t i=0; i<redis_reply->elements; ++i)
        {
            const struct redisReply* value_reply = redis_reply->element[i];
            std::string& value = (*values)[i];

            if (value_reply->type != REDIS_REPLY_NIL)
            {
                value.assign(value_reply->str, value_reply->len);
            }
        }
    }
    return static_cast<int>(redis_reply->elements);
}

int CRedisClient::get_values(const redisReply* redis_reply, std::set<std::string>* values)
{
    values->clear();

    if (redis_reply->elements > 0)
    {
        for (size_t i=0; i<redis_reply->elements; ++i)
        {
            const struct redisReply* value_reply = redis_reply->element[i];

            if (value_reply->type != REDIS_REPLY_NIL)
            {
                const std::string v(value_reply->str, value_reply->len);
                values->insert(v);
            }
        }
    }
    return static_cast<int>(redis_reply->elements);
}

int CRedisClient::get_values(const redisReply* redis_reply, std::vector<std::pair<std::string, int64_t> >* vec, bool withscores)
{
    size_t steps;

    vec->clear();
    if (withscores)
    {
        steps = 2;
        vec->resize(redis_reply->elements/2);
    }
    else
    {
        steps = 1;
        vec->resize(redis_reply->elements);
    }

    for (size_t i=0; i<redis_reply->elements; i+=steps)
    {
        if (!withscores)
        {
            const struct redisReply* v_reply = redis_reply->element[i];
            (*vec)[i].first.assign(v_reply->str, v_reply->len);
            (*vec)[i].second = 0;
        }
        else
        {
            const int j = i / 2;
            const struct redisReply* k_reply = redis_reply->element[i];
            const struct redisReply* v_reply = redis_reply->element[i+1];
            const std::string k(k_reply->str, k_reply->len);
            const std::string v(v_reply->str, v_reply->len);
            (*vec)[j].first = k;
            (*vec)[j].second = static_cast<int64_t>(atoll(v.c_str()));
        }
    }

    return static_cast<int>(vec->size());
}

int CRedisClient::get_values(const redisReply* redis_reply, std::map<std::string, std::string>* map)
{
    map->clear();

    for (size_t i=0; i<redis_reply->elements; i+=2)
    {
        const struct redisReply* f_reply = redis_reply->element[i];
        const struct redisReply* v_reply = redis_reply->element[i+1];
        const std::string f(f_reply->str, f_reply->len);
        const std::string v(v_reply->str, v_reply->len);
        (*map)[f] = v;
    }

    return static_cast<int>(redis_reply->elements/2);
}

int CRedisClient::get_values(const redisReply* redis_reply, const std::vector<std::string>& fields, bool keep_null, std::map<std::string, std::string>* map)
{
    map->clear();

    for (size_t i=0; i<redis_reply->elements; ++i)
    {
        const struct redisReply* value_reply = redis_reply->element[i];

        if (value_reply->type != REDIS_REPLY_NIL)
        {
            (*map)[fields[i]].assign(value_reply->str, value_reply->len);
        }
        else
        {
            if (keep_null)
            {
                (*map)[fields[i]] = "";
            }
        }
    }

    return static_cast<int>(map->size());
}

int CRedisClient::get_values(const redisReply* redis_reply, std::vector<int64_t>* values)
{
    if (NULL == values)
    {
        return -1;
    }
    if (redis_reply->elements > 0)
    {
        values->resize(redis_reply->elements, 0);

        for (size_t i=0; i<redis_reply->elements; ++i)
        {
            (*values)[i] = static_cast<int64_t>(redis_reply->element[i]->integer);
        }
    }

    return static_cast<int>(redis_reply->elements);
}

} // namespace r3c {
