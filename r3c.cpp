// Writed by yijian (eyjian@gmail.com)
// R3C is a C++ client for redis based on hiredis (https://github.com/redis/hiredis)
#include "r3c.h"
#include "utils.h"
#include <assert.h>

#define R3C_ASSERT assert
#define THROW_REDIS_EXCEPTION(errinfo) \
    throw CRedisException(errinfo, __FILE__, __LINE__)
#define THROW_REDIS_EXCEPTION_WITH_NODE(errinfo, node_ip, node_port) \
    throw CRedisException(errinfo, __FILE__, __LINE__, node_ip, node_port)
#define THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(errinfo, node_ip, node_port, command, key) \
    throw CRedisException(errinfo, __FILE__, __LINE__, node_ip, node_port, command, key)

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

static int get_retry_sleep_milliseconds(int loop_counter)
{
    static const int sleep_milliseconds_table[] = { 10, 100, 200, 500, 1000 };
    if (loop_counter<0 || loop_counter>=static_cast<int>(sizeof(sleep_milliseconds_table)/sleep_milliseconds_table[0]-1))
        return 1000;
    else
        return sleep_milliseconds_table[loop_counter];
}

enum
{
    CLUSTER_SLOTS = 16384 // number of slots, defined in cluster.h
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

size_t NodeHasher::operator ()(const Node& node) const
{
    const unsigned char* s = reinterpret_cast<const unsigned char*>(node.first.c_str());
    const uint64_t l = static_cast<uint64_t>(node.first.size());
    uint64_t crc = 0;
    return static_cast<size_t>(crc64(crc, s, l));
}

std::string& node2string(const Node& node, std::string* str)
{
    *str = node.first + int2string(node.second);
    return *str;
}

std::string NodeInfo::str() const
{
    return format_string("nodeinfo://%s/%s:%d/%s", id.c_str(), node.first.c_str(), node.second, flags.c_str());
}

bool NodeInfo::is_master() const
{
    const std::string::size_type pos = flags.find("master");
    return (pos != std::string::npos);
}

bool NodeInfo::is_replica() const
{
    const std::string::size_type pos = flags.find("slave");
    return (pos != std::string::npos);
}

bool NodeInfo::is_fail() const
{
    const std::string::size_type pos = flags.find("fail");
    return (pos != std::string::npos);
}

////////////////////////////////////////////////////////////////////////////////
// CCommandArgs

CommandArgs::CommandArgs()
    : _argc(0), _argv(NULL), _argvlen(NULL)
{
}

CommandArgs::~CommandArgs()
{
    delete []_argvlen;
    for (int i=0; i<_argc; ++i)
        delete []_argv[i];
    delete []_argv;
}

void CommandArgs::set_key(const std::string& key)
{
    _key = key;
}

void CommandArgs::add_arg(const std::string& arg)
{
    _args.push_back(arg);
}

void CommandArgs::add_arg(int32_t arg)
{
    _args.push_back(int2string(arg));
}

void CommandArgs::add_arg(uint32_t arg)
{
    _args.push_back(int2string(arg));
}

void CommandArgs::add_arg(int64_t arg)
{
    _args.push_back(int2string(arg));
}

void CommandArgs::add_args(const std::vector<std::string>& args)
{
    for (std::vector<std::string>::size_type i=0; i<args.size(); ++i)
    {
        const std::string& arg = args[i];
        add_arg(arg);
    }
}

void CommandArgs::add_args(const std::map<std::string, std::string>& map)
{
    for (std::map<std::string, std::string>::const_iterator iter=map.begin(); iter!=map.end(); ++iter)
    {
        add_arg(iter->first);
        add_arg(iter->second);
    }
}

void CommandArgs::add_args(const std::map<std::string, int64_t>& map, bool reverse)
{
    for (std::map<std::string, int64_t>::const_iterator iter=map.begin(); iter!=map.end(); ++iter)
    {
        if (!reverse)
        {
            add_arg(iter->first);
            add_arg(iter->second);
        }
        else
        {
            add_arg(iter->second);
            add_arg(iter->first);
        }
    }
}

void CommandArgs::final()
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

inline int CommandArgs::get_argc() const
{
    return _argc;
}

inline const char** CommandArgs::get_argv() const
{
    return (const char**)_argv;
}

inline const size_t* CommandArgs::get_argvlen() const
{
    return _argvlen;
}

inline const char* CommandArgs::get_command() const
{
    return _argv[0];
}

inline const char* CommandArgs::get_key() const
{
    return !_key.empty()? _key.c_str(): _argv[1];
}

inline size_t CommandArgs::get_command_length() const
{
    return _argvlen[0];
}

inline size_t CommandArgs::get_key_length() const
{
    return _argvlen[1];
}

std::string CommandArgs::get_command_str() const
{
    return std::string(get_command(), get_command_length());
}

std::string CommandArgs::get_key_str() const
{
    return std::string(get_key(), get_key_length());
}

////////////////////////////////////////////////////////////////////////////////
// CRedisNode
// CMasterNode
// CReplica

class CRedisNode
{
public:
    CRedisNode(const Node& node, redisContext* redis_context)
        : _node(node),
          _redis_context(redis_context)
    {
    }

    ~CRedisNode()
    {
        close();
    }

    const Node& get_node() const
    {
        return _node;
    }

    redisContext* get_redis_context() const
    {
        return _redis_context;
    }

    void set_redis_context(redisContext* redis_context)
    {
        _redis_context = redis_context;
    }

    void close()
    {
        if (_redis_context != NULL)
        {
            redisFree(_redis_context);
            _redis_context = NULL;
        }
    }

    std::string str() const
    {
        return format_string("node://%s:%d", _node.first.c_str(), _node.second);
    }

protected:
    Node _node;
    redisContext* _redis_context;
};

class CMasterNode;

class CReplicaNode: public CRedisNode
{
public:
    CReplicaNode(const Node& node, redisContext* redis_context)
        : CRedisNode(node, redis_context),
          _master_node(NULL)
    {
    }

private:
    CMasterNode* _master_node;
};

class CMasterNode: public CRedisNode
{
public:
    CMasterNode(const Node& node, redisContext* redis_context)
        : CRedisNode(node, redis_context)
    {
    }

    ~CMasterNode()
    {
        clear();
    }

    void clear()
    {
        for (ReplicaNodeTable::iterator iter=_replica_nodes.begin(); iter!=_replica_nodes.end(); ++iter)
        {
            CReplicaNode* replica_node = iter->second;
            delete replica_node;
        }
        _replica_nodes.clear();
    }

private:
    typedef std::map<Node, CReplicaNode*> ReplicaNodeTable;
    ReplicaNodeTable _replica_nodes;
};

////////////////////////////////////////////////////////////////////////////////
// RedisReplyHelper

RedisReplyHelper::RedisReplyHelper()
    : _redis_reply(NULL)
{
}

RedisReplyHelper::RedisReplyHelper(const redisReply* redis_reply)
    : _redis_reply(redis_reply)
{
}

RedisReplyHelper::RedisReplyHelper(const RedisReplyHelper& other)
{
    _redis_reply = other.detach();
}

RedisReplyHelper::~RedisReplyHelper()
{
    free();
}

RedisReplyHelper::operator bool() const
{
    return _redis_reply != NULL;
}

void RedisReplyHelper::free()
{
    if (_redis_reply != NULL)
    {
        freeReplyObject((void*)_redis_reply);
        _redis_reply = NULL;
    }
}

const redisReply* RedisReplyHelper::get() const
{
    return _redis_reply;
}

const redisReply* RedisReplyHelper::detach() const
{
    const redisReply* redis_reply = _redis_reply;
    _redis_reply = NULL;
    return redis_reply;
}

RedisReplyHelper& RedisReplyHelper::operator =(const redisReply* redis_reply)
{
    free();
    _redis_reply = redis_reply;
    return *this;
}

RedisReplyHelper& RedisReplyHelper::operator =(const RedisReplyHelper& other)
{
    free();
    _redis_reply = other.detach();
    return *this;
}

const redisReply* RedisReplyHelper::operator ->() const
{
    return _redis_reply;
}

std::ostream& RedisReplyHelper::operator <<(std::ostream& os)
{
    os << *_redis_reply;
    return os;
}

////////////////////////////////////////////////////////////////////////////////
// ErrorInfo

ErrorInfo::ErrorInfo()
    : errcode(0)
{
}

ErrorInfo::ErrorInfo(const std::string& raw_errmsg_, const std::string& errmsg_, const std::string& errtype_, int errcode_)
    : raw_errmsg(raw_errmsg_), errmsg(errmsg_), errtype(errtype_), errcode(errcode_)
{
}

void ErrorInfo::clear()
{
    errcode = 0;
    errtype.clear();
    errmsg.clear();
    raw_errmsg.clear();
}

////////////////////////////////////////////////////////////////////////////////
// CRedisException

CRedisException::CRedisException(
        const struct ErrorInfo& errinfo,
        const char* file, int line,
        const std::string& node_ip, uint16_t node_port,
        const std::string& command,
        const std::string& key) throw ()
    :  _errinfo(errinfo), _line(line), _node_ip(node_ip), _node_port(node_port), _command(command), _key(key)
{
    const char* slash_position = strrchr(file, '/');
    const std::string* file_cp = &_file;
    std::string* file_p = const_cast<std::string*>(file_cp);

    if (NULL == slash_position)
    {
        *file_p = file;
    }
    else
    {
        *file_p = std::string(slash_position + 1);
    }
}

const char* CRedisException::what() const throw()
{
    return _errinfo.errmsg.c_str();
}

std::string CRedisException::str() const throw ()
{
    return format_string("redis://%s:%d/CMD:%s/KEY:%.*s/%s/(%d)%s@%s:%d",
            _node_ip.c_str(), _node_port,
            _command.c_str(), static_cast<int>(_key.length()), _key.c_str(),
            _errinfo.errtype.c_str(), _errinfo.errcode, _errinfo.errmsg.c_str(),
            _file.c_str(), _line);
}

bool is_ask_error(const std::string& errtype)
{
    return (errtype.size() == sizeof("ASK")-1) && (errtype == "ASK");
}

bool is_clusterdown_error(const std::string& errtype)
{
    return (errtype.size() == sizeof("CLUSTERDOWN")-1) && (errtype == "CLUSTERDOWN");
}

bool is_moved_error(const std::string& errtype)
{
    return (errtype.size() == sizeof("MOVED")-1) && (errtype == "MOVED");
}

bool is_noauth_error(const std::string& errtype)
{
    // NOAUTH Authentication required.
    return (errtype.size() == sizeof("NOAUTH")-1) && (errtype == "NOAUTH");
}

bool is_noscript_error(const std::string& errtype)
{
    // NOSCRIPT No matching script. Please use EVAL.
    return (errtype.size() == sizeof("NOSCRIPT")-1) && (errtype == "NOSCRIPT");
}

bool is_wrongtype_error(const std::string& errtype)
{
    return (errtype.size() == sizeof("WRONGTYPE")-1) && (errtype == "WRONGTYPE");
}

////////////////////////////////////////////////////////////////////////////////
// CRedisClient

CRedisClient::CRedisClient(
        const std::string& raw_nodes_string,
        int connect_timeout_milliseconds,
        int readwrite_timeout_milliseconds,
        const std::string& password,
        ReadPolicy read_policy
        ) throw (CRedisException)
            : _command_monitor(NULL),
              _raw_nodes_string(raw_nodes_string),
              _connect_timeout_milliseconds(connect_timeout_milliseconds),
              _readwrite_timeout_milliseconds(readwrite_timeout_milliseconds),
              _password(password),
              _read_policy(read_policy)
{
    init();
}

CRedisClient::CRedisClient(
        const std::string& raw_nodes_string,
        const std::string& password,
        int connect_timeout_milliseconds,
        int readwrite_timeout_milliseconds,
        ReadPolicy read_policy) throw (CRedisException)
            : _command_monitor(NULL),
              _raw_nodes_string(raw_nodes_string),
              _connect_timeout_milliseconds(connect_timeout_milliseconds),
              _readwrite_timeout_milliseconds(readwrite_timeout_milliseconds),
              _password(password),
              _read_policy(read_policy)
{
    init();
}

CRedisClient::~CRedisClient()
{
    clear_all_master_nodes();
}

const std::string& CRedisClient::get_raw_nodes_string() const
{
    return _raw_nodes_string;
}

const std::string& CRedisClient::get_master_nodes_string() const
{
    return _master_nodes_string;
}

std::string CRedisClient::str() const
{
    return std::string("redis://") + _raw_nodes_string;
}

bool CRedisClient::cluster_mode() const
{
    return _nodes.size() > 1;
}

int CRedisClient::list_nodes(std::vector<struct NodeInfo>* nodes_info) throw (CRedisException)
{
    struct ErrorInfo errinfo;

    for (MasterNodeTable::iterator iter=_master_nodes.begin(); iter!=_master_nodes.end(); ++iter)
    {
        const Node& node = iter->first;
        struct CRedisNode* redis_node = iter->second;
        redisContext* redis_context = redis_node->get_redis_context();

        if (redis_context != NULL)
        {
            if (list_cluster_nodes(nodes_info, &errinfo, redis_context, node))
                break;
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
    const int num_retries = NUM_RETRIES;
    const std::string key;
    CommandArgs cmd_args;
    cmd_args.add_arg("FLUSHALL");
    cmd_args.final();

    // Simple string reply (REDIS_REPLY_STATUS)
    redis_command(true, num_retries, key, cmd_args, NULL);
}

void CRedisClient::multi(const std::string& key, Node* which)
{
    const int num_retries = 0;
    CommandArgs cmd_args;
    cmd_args.set_key(key);
    cmd_args.add_arg("MULTI");
    cmd_args.final();

    // Simple string reply (REDIS_REPLY_STATUS):
    // always OK.
    redis_command(false, num_retries, key, cmd_args, which);
}

const RedisReplyHelper CRedisClient::exec(const std::string& key, Node* which)
{
    const int num_retries = 0;
    CommandArgs cmd_args;
    cmd_args.set_key(key);
    cmd_args.add_arg("EXEC");
    cmd_args.final();

    // Array reply:
    // each element being the reply to each of the commands in the atomic transaction.
    return redis_command(false, num_retries, key, cmd_args, which);
}

//
// KEY/VALUE
//

// Time complexity: O(1)
// EXPIRE key seconds
bool CRedisClient::expire(
        const std::string& key,
        uint32_t seconds,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("EXPIRE");
    cmd_args.add_arg(key);
    cmd_args.add_arg(seconds);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if the timeout was set.
    // 0 if key does not exist.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// Time complexity: O(1)
// EXISTS key [key ...]
bool CRedisClient::exists(const std::string& key, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("EXISTS");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if the key exists.
    // 0 if the key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// Time complexity:
// O(N) where N is the number of keys that will be removed.
// When a key to remove holds a value other than a string,
// the individual complexity for this key is O(M) where M is the number of elements in the list, set, sorted set or hash.
// Removing a single key that holds a string value is O(1).
bool CRedisClient::del(const std::string& key, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("DEL");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply:
    // The number of keys that were removed.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// GET key
// Time complexity: O(1)
bool CRedisClient::get(
        const std::string& key,
        std::string* value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("GET");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Bulk string reply:
    // the value of key, or nil when key does not exist.
    RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return false;
    if (REDIS_REPLY_STRING == redis_reply->type)
        return get_value(redis_reply.get(), value);
    return true;
}

// SET key value [EX seconds] [PX milliseconds] [NX|XX]
// Time complexity: O(1)
void CRedisClient::set(
        const std::string& key,
        const std::string& value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
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
    redis_command(false, num_retries, key, cmd_args, which);
}

// Time complexity: O(1)
// SETNX key value
bool CRedisClient::setnx(
        const std::string& key,
        const std::string& value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("SETNX");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if the key exists.
    // 0 if the key does not exist.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// Time complexity: O(1)
// SETEX key seconds value
void CRedisClient::setex(
        const std::string& key,
        const std::string& value,
        uint32_t expired_seconds,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("SETEX");
    cmd_args.add_arg(key);
    cmd_args.add_arg(expired_seconds);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Simple string reply (REDIS_REPLY_STATUS)
    //
    // OK: redis_reply->str
    redis_command(false, num_retries, key, cmd_args, which);
}

bool CRedisClient::setnxex(
        const std::string& key,
        const std::string& value,
        uint32_t expired_seconds,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
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
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    return redis_reply->type != REDIS_REPLY_NIL;
}

// Time complexity: O(N) where N is the number of keys to retrieve.
// MGET key [key ...]
int CRedisClient::mget(
        const std::vector<std::string>& keys,
        std::vector<std::string>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    values->clear();

    if (!cluster_mode())
    {
        const std::string key;
        CommandArgs cmd_args;
        cmd_args.add_arg("MGET");
        cmd_args.add_args(keys);
        cmd_args.final();

        // Array reply:
        // list of values at the specified keys.
        //
        // For every key that does not hold a string value or does not exist,
        // the special value nil is returned. Because of this, the operation never fails.
        const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
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
                get(key, &value, NULL, num_retries);
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
int CRedisClient::mset(
        const std::map<std::string, std::string>& kv_map,
        Node* which,
        int num_retries) throw (CRedisException)
{
    int success = 0;

    if (!cluster_mode())
    {
        const std::string key;
        CommandArgs cmd_args;
        cmd_args.add_arg("MSET");
        cmd_args.add_args(kv_map);
        cmd_args.final();

        // Simple string reply:
        // always OK since MSET can't fail.
        redis_command(false, num_retries, key, cmd_args, which);
        success = static_cast<int>(kv_map.size());
    }
    else
    {
        for (std::map<std::string, std::string>::const_iterator iter=kv_map.begin(); iter!=kv_map.end(); ++iter)
        {
            const std::string& key = iter->first;
            const std::string& value = iter->second;
            set(key, value, which ,num_retries);
            ++success;
        }
    }

    return success;
}

// Time complexity: O(1)
// INCRBY key increment
int64_t CRedisClient::incrby(
        const std::string& key,
        int64_t increment,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("INCRBY");
    cmd_args.add_arg(key);
    cmd_args.add_arg(increment);
    cmd_args.final();

    // Integer reply:
    // the value of key after the increment
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return redis_reply->integer;
    return 0;
}

int64_t CRedisClient::incrby(
        const std::string& key,
        int64_t increment, int64_t expired_increment, uint32_t expired_seconds,
        Node* which,
        int num_retries) throw (CRedisException)
{
    // 注意ARGV[2]和n需类型相同才可以比较，所以要么n转成字符串，要么ARGV[2]转成数字
    const std::string lua_scripts = "local n;n=redis.call('INCRBY',KEYS[1],ARGV[1]);if (n==tonumber(ARGV[2])) then redis.call('EXPIRE',KEYS[1],ARGV[3]) end;return n;";
    std::vector<std::string> parameters(3);
    parameters[0] = int2string(increment);
    parameters[1] = int2string(expired_increment);
    parameters[2] = int2string(expired_seconds);
    const RedisReplyHelper redis_reply = eval(key, lua_scripts, parameters, which, num_retries);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int64_t>(redis_reply->integer);
    return 0;
}

int64_t CRedisClient::incrby(
        const std::string& key,
        int64_t increment, uint32_t expired_seconds,
        Node* which,
        int num_retries) throw (CRedisException)
{
    const int64_t expired_increment = increment;
    return incrby(key, increment, expired_increment, expired_seconds, which, num_retries);
}

bool CRedisClient::key_type(const std::string& key, std::string* key_type, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("TYPE");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Simple string reply (REDIS_REPLY_STATUS):
    // type of key, or none when key does not exist.
    //
    // (gdb) p *redis_reply._redis_reply
    // $1 = {type = 5, integer = 0, len = 6, str = 0x742b50 "string", elements = 0, element = 0x0}
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    return get_value(redis_reply.get(), key_type);
}

int64_t CRedisClient::ttl(const std::string& key, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("TTL");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply:
    // TTL in seconds, or a negative value in order to signal an error
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return redis_reply->integer;
    return 0;
}

// O(1) for every call. O(N) for a complete iteration,
// including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection.
//
// SCAN cursor [MATCH pattern] [COUNT count]
int64_t CRedisClient::scan(int64_t cursor, std::vector<std::string>* values, Node* which, int num_retries) throw (CRedisException)
{
    return scan(cursor, std::string(""), 0, values, which, num_retries);
}

// SCAN cursor [MATCH pattern] [COUNT count]
int64_t CRedisClient::scan(int64_t cursor, int count, std::vector<std::string>* values, Node* which, int num_retries) throw (CRedisException)
{
    return scan(cursor, std::string(""), count, values, which, num_retries);
}

// SCAN cursor [MATCH pattern] [COUNT count]
int64_t CRedisClient::scan(
        int64_t cursor, const std::string& pattern,
        std::vector<std::string>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    return scan(cursor, pattern, 0, values, which, num_retries);
}

// SCAN cursor [MATCH pattern] [COUNT count]
int64_t CRedisClient::scan(
        int64_t cursor, const std::string& pattern, int count,
        std::vector<std::string>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    const std::string key;
    CommandArgs cmd_args;
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
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
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
// Time complexity: Depends on the script that is executed.
// EVAL script numkeys key [key ...] arg [arg ...]
//
// e.g.
// eval("r3c_k1", "local v=redis.call('set','r3c_k1','123');return v;");
// eval("r3c_k1", "local v=redis.call('get','r3c_k1');return v;");
const RedisReplyHelper CRedisClient::eval(
        const std::string& key,
        const std::string& lua_scripts,
        Node* which,
        int num_retries) throw (CRedisException)
{
    const int numkeys = 1;
    CommandArgs cmd_args;
    cmd_args.set_key(key);
    cmd_args.add_arg("EVAL");
    cmd_args.add_arg(lua_scripts);
    cmd_args.add_arg(numkeys);
    cmd_args.add_arg(key);
    cmd_args.final();

    return redis_command(false, num_retries, key, cmd_args, which);
}

const RedisReplyHelper CRedisClient::eval(
        const std::string& key,
        const std::string& lua_scripts,
        const std::vector<std::string>& parameters,
        Node* which,
        int num_retries) throw (CRedisException)
{
    const int numkeys = 1;
    CommandArgs cmd_args;
    cmd_args.set_key(key);
    cmd_args.add_arg("EVAL");
    cmd_args.add_arg(lua_scripts);
    cmd_args.add_arg(numkeys);
    cmd_args.add_arg(key);
    cmd_args.add_args(parameters);
    cmd_args.final();

    return redis_command(false, num_retries, key, cmd_args, which);
}

const RedisReplyHelper CRedisClient::evalsha(
        const std::string& key,
        const std::string& sha1,
        const std::vector<std::string>& parameters,
        Node* which, int num_retries) throw (CRedisException)
{
    const int numkeys = 1;
    CommandArgs cmd_args;
    cmd_args.set_key(key);
    cmd_args.add_arg("EVALSHA");
    cmd_args.add_arg(sha1);
    cmd_args.add_arg(numkeys);
    cmd_args.add_arg(key);
    cmd_args.add_args(parameters);
    cmd_args.final();

    return redis_command(false, num_retries, key, cmd_args, which);
}

const RedisReplyHelper CRedisClient::eval(
        const std::string& lua_scripts,
        const std::vector<std::string>& keys, const std::vector<std::string>& parameters,
        Node* which,
        int num_retries) throw (CRedisException)
{
    const int numkeys = static_cast<int>(keys.size());
    const std::string key = std::string("");
    CommandArgs cmd_args;
    cmd_args.add_arg("EVAL");
    cmd_args.add_arg(lua_scripts);
    cmd_args.add_arg(numkeys);
    cmd_args.add_args(keys);
    cmd_args.add_args(parameters);
    cmd_args.final();

    return redis_command(false, num_retries, key, cmd_args, which);
}

const RedisReplyHelper CRedisClient::evalsha(
        const std::string& sha1,
        const std::vector<std::string>& keys,
        const std::vector<std::string>& parameters,
        Node* which,
        int num_retries) throw (CRedisException)
{
    const int numkeys = static_cast<int>(keys.size());
    const std::string key = std::string("");
    CommandArgs cmd_args;
    cmd_args.add_arg("EVALSHA");
    cmd_args.add_arg(sha1);
    cmd_args.add_arg(numkeys);
    cmd_args.add_args(keys);
    cmd_args.add_args(parameters);
    cmd_args.final();

    return redis_command(false, num_retries, key, cmd_args, which);
}

//
// HASH
//

// Time complexity: O(N) where N is the number of fields to be removed.
// HDEL key field [field ...]
bool CRedisClient::hdel(const std::string& key, const std::string& field, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HDEL");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // Integer reply:
    // the number of fields that were removed from the hash, not including specified but non existing fields.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return false;
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

int CRedisClient::hdel(
        const std::string& key,
        const std::vector<std::string>& fields,
        Node* which,
        int num_retries) throw (CRedisException)
{
    return hmdel(key, fields, which, num_retries);
}

int CRedisClient::hmdel(
        const std::string& key,
        const std::vector<std::string>& fields,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HDEL");
    cmd_args.add_arg(key);
    cmd_args.add_args(fields);
    cmd_args.final();

    // Integer reply:
    // the number of fields that were removed from the hash, not including specified but non existing fields.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return 0;
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
// HEXISTS key field
bool CRedisClient::hexists(const std::string& key, const std::string& field, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HEXISTS");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if the hash contains field.
    // 0 if the hash does not contain field, or key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// Time complexity: O(1)
// HLEN key
int CRedisClient::hlen(const std::string& key, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HLEN");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply: number of fields in the hash, or 0 when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
// HSET key field value
bool CRedisClient::hset(
        const std::string& key,
        const std::string& field,
        const std::string& value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HSET");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if field is a new field in the hash and value was set.
    // 0 if field already exists in the hash and the value was updated.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

bool CRedisClient::hsetex(
        const std::string& key,
        const std::string& field,
        const std::string& value,
        uint32_t expired_seconds,
        Node* which,
        int num_retries) throw (CRedisException)
{
    const std::string lua_scripts =
            "local n;n=redis.call('HSET',KEYS[1],ARGV[1],ARGV[2]);if (n>0) then redis.call('EXPIRE',KEYS[1],ARGV[3]) end;return n;";
    std::vector<std::string> parameters(3);
    parameters[0] = field;
    parameters[1] = value;
    parameters[2] = int2string(expired_seconds);

    const RedisReplyHelper redis_reply = eval(key, lua_scripts, parameters, which, num_retries);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// HSETNX key field value
// Time complexity: O(1)
bool CRedisClient::hsetnx(
        const std::string& key,
        const std::string& field,
        const std::string& value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HSETNX");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if field is a new field in the hash and value was set.
    // 0 if field already exists in the hash and no operation was performed.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

bool CRedisClient::hsetnxex(
        const std::string& key,
        const std::string& field,
        const std::string& value,
        uint32_t expired_seconds,
        Node* which,
        int num_retries) throw (CRedisException)
{
    const std::string lua_scripts =
            "local n;n=redis.call('HSETNX',KEYS[1],ARGV[1],ARGV[2]);if (n>0) then redis.call('EXPIRE',KEYS[1],ARGV[3]) end;return n;";
    std::vector<std::string> parameters(3);
    parameters[0] = field;
    parameters[1] = value;
    parameters[2] = int2string(expired_seconds);

    const RedisReplyHelper redis_reply = eval(key, lua_scripts, parameters, which, num_retries);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// Time complexity: O(1)
// HGET key field
bool CRedisClient::hget(
        const std::string& key,
        const std::string& field,
        std::string* value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HGET");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // Bulk string reply:
    // the value associated with field, or nil when field is not present in the hash or key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return false;
    if (REDIS_REPLY_STRING == redis_reply->type)
        return get_value(redis_reply.get(), value);
    return true;
}

// Time complexity: O(1)
// HINCRBY key field increment
int64_t CRedisClient::hincrby(
        const std::string& key,
        const std::string& field,
        int64_t increment,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HINCRBY");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.add_arg(increment);
    cmd_args.final();

    // Integer reply: the value at field after the increment operation.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int64_t>(redis_reply->integer);
    return 0;
}

void CRedisClient::hincrby(
        const std::string& key,
        const std::vector<std::pair<std::string, int64_t> >& increments,
        std::vector<int64_t>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    hmincrby(key, increments, values, which, num_retries);
}

void CRedisClient::hmincrby(
        const std::string& key,
        const std::vector<std::pair<std::string, int64_t> >& increments,
        std::vector<int64_t>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    const std::string lua_scripts =
            "local j=1;local results={};for i=1,#ARGV,2 do local f=ARGV[i];local v=ARGV[i+1];results[j]=redis.call('HINCRBY',KEYS[1],f,v);j=j+1; end;return results;";
    std::vector<std::string> parameters(2*increments.size());
    for (std::vector<std::pair<std::string, int64_t> >::size_type i=0,j=0; i<increments.size(); ++i,j+=2)
    {
        const std::pair<std::string, int64_t>& increment = increments[i];
        parameters[j] = increment.first;
        parameters[j+1] = int2string(increment.second);
    }
    const RedisReplyHelper redis_reply = eval(key, lua_scripts, parameters, which, num_retries);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        get_values(redis_reply.get(), values);
}

void CRedisClient::hset(
        const std::string& key,
        const std::map<std::string, std::string>& map,
        Node* which,
        int num_retries) throw (CRedisException)
{
    hmset(key, map, which, num_retries);
}

// Time complexity: O(N) where N is the number of fields being set.
// HMSET key field value [field value ...]
void CRedisClient::hmset(
        const std::string& key,
        const std::map<std::string, std::string>& map,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HMSET");
    cmd_args.add_arg(key);
    cmd_args.add_args(map);
    cmd_args.final();

    // Simple string reply (REDIS_REPLY_STATUS)
    redis_command(false, num_retries, key, cmd_args, which);
}

int CRedisClient::hget(
        const std::string& key,
        const std::vector<std::string>& fields,
        std::map<std::string, std::string>* map,
        bool keep_null,
        Node* which,
        int num_retries) throw (CRedisException)
{
    return hmget(key, fields, map, keep_null, which, num_retries);
}

// Time complexity: O(N) where N is the number of fields being requested.
// HMGET key field [field ...]
int CRedisClient::hmget(
        const std::string& key,
        const std::vector<std::string>& fields,
        std::map<std::string, std::string>* map,
        bool keep_null,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HMGET");
    cmd_args.add_arg(key);
    cmd_args.add_args(fields);
    cmd_args.final();

    // Array reply:
    // list of values associated with the given fields, in the same order as they are requested.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return 0;
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), fields, keep_null, map);
    return 0;
}

// Time complexity: O(N) where N is the size of the hash.
// HGETALL key
int CRedisClient::hgetall(
        const std::string& key,
        std::map<std::string, std::string>* map,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HGETALL");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Array reply:
    // list of fields and their values stored in the hash, or an empty list when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return 0;
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), map);
    return 0;
}

// Time complexity: O(1)
// HSTRLEN key field
int CRedisClient::hstrlen(const std::string& key, const std::string& field, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HSTRLEN");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // Integer reply:
    // the string length of the value associated with field,
    // or zero when field is not present in the hash or key does not exist at all.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return 0;
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(N) where N is the size of the hash.
// HKEYS key
int CRedisClient::hkeys(const std::string& key, std::vector<std::string>* fields, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HKEYS");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Array reply:
    // list of fields in the hash, or an empty list when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return 0;
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), fields);
    return 0;
}

// Time complexity: O(N) where N is the size of the hash.
// HVALS key
int CRedisClient::hvals(const std::string& key, std::vector<std::string>* vals, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("HVALS");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Array reply:
    // list of values in the hash, or an empty list when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
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
int64_t CRedisClient::hscan(
        const std::string& key,
        int64_t cursor,
        std::map<std::string, std::string>* map,
        Node* which,
        int num_retries) throw (CRedisException)
{
    return hscan(key, cursor, std::string(""), 0, map, which, num_retries);
}

int64_t CRedisClient::hscan(
        const std::string& key,
        int64_t cursor, int count,
        std::map<std::string, std::string>* map,
        Node* which,
        int num_retries) throw (CRedisException)
{
    return hscan(key, cursor, std::string(""), count, map, which, num_retries);
}

int64_t CRedisClient::hscan(
        const std::string& key,
        int64_t cursor, const std::string& pattern,
        std::map<std::string, std::string>* map,
        Node* which,
        int num_retries) throw (CRedisException)
{
    return hscan(key, cursor, pattern, 0, map, which, num_retries);
}

int64_t CRedisClient::hscan(
        const std::string& key,
        int64_t cursor, const std::string& pattern, int count,
        std::map<std::string, std::string>* map,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
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
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
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
int CRedisClient::llen(const std::string& key, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("LLEN");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply: the length of the list at key.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
// LPOP key
bool CRedisClient::lpop(const std::string& key, std::string* value, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("LPOP");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Bulk string reply: the value of the first element, or nil when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return false;
    if (REDIS_REPLY_STRING == redis_reply->type)
        return get_value(redis_reply.get(), value);
    return true; // MULTI & EXEC the type always is REDIS_REPLY_STATUS
}

// Time complexity: O(1)
// LPUSH key value [value ...]
int CRedisClient::lpush(
        const std::string& key,
        const std::string& value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("LPUSH");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply: the length of the list after the push operations.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
// LPUSH key value [value ...]
int CRedisClient::lpush(
        const std::string& key,
        const std::vector<std::string>& values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("LPUSH");
    cmd_args.add_arg(key);
    cmd_args.add_args(values);
    cmd_args.final();

    // Integer reply: the length of the list after the push operations.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
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
int CRedisClient::lrange(
        const std::string& key,
        int64_t start, int64_t end,
        std::vector<std::string>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("LRANGE");
    cmd_args.add_arg(key);
    cmd_args.add_arg(start);
    cmd_args.add_arg(end);
    cmd_args.final();

    // Array reply: list of elements in the specified range.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), values);
    return 0;
}

// Time complexity:
// O(N) where N is the number of elements to be removed by the operation.
//
// LTRIM key start stop
void CRedisClient::ltrim(
        const std::string& key,
        int64_t start, int64_t end,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("LTRIM");
    cmd_args.add_arg(key);
    cmd_args.add_arg(start);
    cmd_args.add_arg(end);
    cmd_args.final();

    // Simple string reply (REDIS_REPLY_STATUS)
    redis_command(false, num_retries, key, cmd_args, which);
}

// Time complexity: O(1)
// RPOP key
bool CRedisClient::rpop(
        const std::string& key,
        std::string* value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("RPOP");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Bulk string reply: the value of the last element, or nil when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return false;
    if (REDIS_REPLY_STRING == redis_reply->type)
        return get_value(redis_reply.get(), value);
    return true;
}

// Time complexity: O(1)
int CRedisClient::rpush(
        const std::string& key,
        const std::string& value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("RPUSH");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply: the length of the list after the push operation.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
int CRedisClient::rpush(
        const std::string& key,
        const std::vector<std::string>& values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("RPUSH");
    cmd_args.add_arg(key);
    cmd_args.add_args(values);
    cmd_args.final();

    // Integer reply: the length of the list after the push operation.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
int CRedisClient::rpushx(
        const std::string& key,
        const std::string& value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("RPUSHX");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply:
    // the length of the list after the push operation.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

//
// SET
//

// Time complexity:
//  O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
int CRedisClient::sadd(
        const std::string& key,
        const std::string& value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("SADD");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply: the number of elements that were added to the set,
    // not including all the elements already present into the set.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

//  O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
int CRedisClient::sadd(
        const std::string& key,
        const std::vector<std::string>& values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("SADD");
    cmd_args.add_arg(key);
    cmd_args.add_args(values);
    cmd_args.final();

    // Integer reply: the number of elements that were added to the set,
    // not including all the elements already present into the set.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// O(1)
int CRedisClient::scard(const std::string& key, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("SCARD");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply: the cardinality (number of elements) of the set, or 0 if key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// O(1)
bool CRedisClient::sismember(
        const std::string& key,
        const std::string& value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("SISMEMBER");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply, specifically:
    // 1 if the element is a member of the set.
    // 0 if the element is not a member of the set, or if key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

// O(N) where N is the set cardinality.
int CRedisClient::smembers(
        const std::string& key,
        std::vector<std::string>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("SMEMBERS");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Array reply:
    // all elements of the set.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), values);
    return 0;
}

// Time complexity: O(1)
bool CRedisClient::spop(
        const std::string& key,
        std::string* value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    std::vector<std::string> values;
    const int n = spop(key, 1, &values, which, num_retries);
    if (n > 0)
    {
        *value = value[0];
    }
    return n > 0;
}

// Time complexity: O(1)
int CRedisClient::spop(
        const std::string& key,
        int count,
        std::vector<std::string>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("SPOP");
    cmd_args.add_arg(key);
    cmd_args.add_arg(count);
    cmd_args.final();

    // Bulk string reply:
    // the removed element, or nil when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), values);
    return 0;
}

// Time complexity: O(1)
bool CRedisClient::srandmember(
        const std::string& key,
        std::string* value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("SRANDMEMBER");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Bulk string reply:
    // without the additional count argument the command returns a Bulk Reply with the randomly selected element,
    // or nil when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_NIL == redis_reply->type)
        return false;
    if (REDIS_REPLY_STRING == redis_reply->type)
        return get_value(redis_reply.get(), value);
    return true;
}

// Time complexity: O(N) where N is the absolute value of the passed count.
int CRedisClient::srandmember(
        const std::string& key,
        int count,
        std::vector<std::string>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("SRANDMEMBER");
    cmd_args.add_arg(key);
    cmd_args.add_arg(count);
    cmd_args.final();

    // Array reply:
    // when the additional count argument is passed the command returns an array of elements,
    // or an empty array when key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), values);
    return 0;
}

// Time complexity: O(N) where N is the number of members to be removed.
int CRedisClient::srem(
        const std::string& key,
        const std::string& value,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("SREM");
    cmd_args.add_arg(key);
    cmd_args.add_arg(value);
    cmd_args.final();

    // Integer reply: the number of members that were removed from the set,
    // not including non existing members.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

int CRedisClient::srem(
        const std::string& key,
        const std::vector<std::string>& values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("SREM");
    cmd_args.add_arg(key);
    cmd_args.add_args(values);
    cmd_args.final();

    // Integer reply: the number of members that were removed from the set,
    // not including non existing members.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity:
// O(1) for every call.
// O(N) for a complete iteration, including enough command calls for the cursor to return back to 0.
// N is the number of elements inside the collection..
int64_t CRedisClient::sscan(
        const std::string& key,
        int64_t cursor,
        std::vector<std::string>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    return sscan(key, cursor, std::string(""), 0, values, which, num_retries);
}

int64_t CRedisClient::sscan(
        const std::string& key,
        int64_t cursor, int count,
        std::vector<std::string>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    return sscan(key, cursor, std::string(""), count, values, which, num_retries);
}

int64_t CRedisClient::sscan(
        const std::string& key,
        int64_t cursor, const std::string& pattern,
        std::vector<std::string>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    return sscan(key, cursor, pattern, 0, values, which, num_retries);
}

int64_t CRedisClient::sscan(
        const std::string& key,
        int64_t cursor, const std::string& pattern, int count,
        std::vector<std::string>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    // SSCAN key cursor [MATCH pattern] [COUNT count]
    CommandArgs cmd_args;
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
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
    {
        get_values(redis_reply->element[1], values);
        return static_cast<int64_t>(atoll(redis_reply->element[0]->str));
    }
    return 0;
}

int64_t CRedisClient::sscan(
        const std::string& key,
        int64_t cursor,
        const std::string& pattern,
        int count,
        std::set<std::string>* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    // SSCAN key cursor [MATCH pattern] [COUNT count]
    CommandArgs cmd_args;
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

    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
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
int CRedisClient::zrem(
        const std::string& key,
        const std::string& field,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("ZREM");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // Integer reply, specifically:
    // The number of members removed from the sorted set, not including non existing members.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity:
// O(M*log(N)) with N being the number of elements in the sorted set and M the number of elements to be removed.
int CRedisClient::zrem(
        const std::string& key,
        const std::vector<std::string>& fields,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("ZREM");
    cmd_args.add_arg(key);
    cmd_args.add_args(fields);
    cmd_args.final();

    // Integer reply, specifically:
    // The number of members removed from the sorted set, not including non existing members.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity:
// O(log(N)) for each item added, where N is the number of elements in the sorted set.
//
// ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
int CRedisClient::zadd(
        const std::string& key,
        const std::string& field,
        int64_t score,
        ZADDFLAG flag,
        Node* which,
        int num_retries) throw (CRedisException)
{
    std::map<std::string, int64_t> map;
    map[field] = score;
    return zadd(key, map, flag, which, num_retries);
}

int CRedisClient::zadd(
        const std::string& key,
        const std::map<std::string, int64_t>& map,
        ZADDFLAG flag,
        Node* which,
        int num_retries) throw (CRedisException)
{
    const std::string& flag_str = zaddflag2str(flag);
    CommandArgs cmd_args;
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
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(1)
int64_t CRedisClient::zcard(const std::string& key, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("ZCARD");
    cmd_args.add_arg(key);
    cmd_args.final();

    // Integer reply:
    // the cardinality (number of elements) of the sorted set, or 0 if key does not exist.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int64_t>(redis_reply->integer);
    return 0;
}

// Time complexity:
// O(log(N)) with N being the number of elements in the sorted set.
//
// ZCOUNT key min max
int64_t CRedisClient::zcount(
        const std::string& key,
        int64_t min, int64_t max ,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("ZCOUNT");
    cmd_args.add_arg(key);
    cmd_args.add_arg(min);
    cmd_args.add_arg(max);
    cmd_args.final();

    // Integer reply:
    // the number of elements in the specified score range.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int64_t>(redis_reply->integer);
    return 0;
}

// Time complexity:
// O(log(N)) where N is the number of elements in the sorted set.
//
// ZINCRBY key increment member
int64_t CRedisClient::zincrby(
        const std::string& key,
        const std::string& field, int64_t increment,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
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
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_STRING == redis_reply->type)
        return static_cast<int64_t>(atoll(redis_reply->str));
    return 0;
}

// Time complexity:
// O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned.
// ZRANGE key start stop [WITHSCORES]
//
// ZRANGE key start stop [WITHSCORES]
int CRedisClient::zrange(
        const std::string& key,
        int64_t start, int64_t end, bool withscores,
        std::vector<std::pair<std::string, int64_t> >* vec,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
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
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), vec, withscores);
    return 0;
}

// Time complexity:
// O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned.
//
// ZREVRANGE key start stop [WITHSCORES]
int CRedisClient::zrevrange(
        const std::string& key,
        int64_t start, int64_t end, bool withscores,
        std::vector<std::pair<std::string, int64_t> >* vec,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
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
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), vec, withscores);
    return 0;
}

// Time complexity:
// O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned.
// If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
//
// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
int CRedisClient::zrangebyscore(
        const std::string& key,
        int64_t min, int64_t max, bool withscores,
        std::vector<std::pair<std::string, int64_t> >* vec,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
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
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), vec, withscores);
    return 0;
}

// Time complexity:
// O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned.
// If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
//
// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
int CRedisClient::zrevrangebyscore(
        const std::string& key,
        int64_t max, int64_t min, bool withscores,
        std::vector<std::pair<std::string, int64_t> >* vec,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
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
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), vec, withscores);
    return 0;
}

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
int CRedisClient::zrangebyscore(
        const std::string& key,
        int64_t min, int64_t max, int64_t offset, int64_t count, bool withscores,
        std::vector<std::pair<std::string, int64_t> >* vec,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
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
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), vec, withscores);
    return 0;
}

// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
int CRedisClient::zrevrangebyscore(
        const std::string& key,
        int64_t max, int64_t min, int64_t offset, int64_t count, bool withscores,
        std::vector<std::pair<std::string, int64_t> >* vec,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
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
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        return get_values(redis_reply.get(), vec, withscores);
    return 0;
}

// Time complexity:
// O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements removed by the operation.
//
// ZREMRANGEBYRANK key start stop
int CRedisClient::zremrangebyrank(
        const std::string& key,
        int64_t start, int64_t end,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("ZREMRANGEBYRANK");
    cmd_args.add_arg(key);
    cmd_args.add_arg(start);
    cmd_args.add_arg(end);
    cmd_args.final();

    // Integer reply:
    // the number of elements removed.
    const RedisReplyHelper redis_reply = redis_command(false, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return static_cast<int>(redis_reply->integer);
    return 0;
}

// Time complexity: O(log(N))
//
// ZRANK key member
int CRedisClient::zrank(const std::string& key, const std::string& field, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("ZRANK");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // If member exists in the sorted set, Integer reply: the rank of member.
    // If member does not exist in the sorted set or key does not exist, Bulk string reply: nil.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
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

int CRedisClient::zrevrank(const std::string& key, const std::string& field, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("ZREVRANK");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // If member exists in the sorted set, Integer reply: the rank of member.
    // If member does not exist in the sorted set or key does not exist, Bulk string reply: nil.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
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
int64_t CRedisClient::zscore(const std::string& key, const std::string& field, Node* which, int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
    cmd_args.add_arg("ZSCORE");
    cmd_args.add_arg(key);
    cmd_args.add_arg(field);
    cmd_args.final();

    // Bulk string reply:
    // the score of member (a double precision floating point number), represented as string.
    // If member does not exist in the sorted set, or key does not exist, nil is returned.
    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
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
int64_t CRedisClient::zscan(
        const std::string& key,
        int64_t cursor,
        std::vector<std::pair<std::string, int64_t> >* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    return zscan(key, cursor, std::string(""), 0, values, which, num_retries);
}

int64_t CRedisClient::zscan(
        const std::string& key,
        int64_t cursor, int count,
        std::vector<std::pair<std::string, int64_t> >* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    return zscan(key, cursor, std::string(""), count, values, which, num_retries);
}

int64_t CRedisClient::zscan(
        const std::string& key,
        int64_t cursor, const std::string& pattern,
        std::vector<std::pair<std::string, int64_t> >* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    return zscan(key, cursor, pattern, 0, values, which, num_retries);
}

int64_t CRedisClient::zscan(
        const std::string& key,
        int64_t cursor, const std::string& pattern, int count,
        std::vector<std::pair<std::string, int64_t> >* values,
        Node* which,
        int num_retries) throw (CRedisException)
{
    CommandArgs cmd_args;
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

    const RedisReplyHelper redis_reply = redis_command(true, num_retries, key, cmd_args, which);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
    {
        get_values(redis_reply->element[1], values, true);
        return static_cast<int64_t>(atoll(redis_reply->element[0]->str));
    }
    return 0;
}

const RedisReplyHelper
CRedisClient::redis_command(
        bool readonly, int num_retries,
        const std::string& key, const CommandArgs& command_args,
        Node* which)
{
    Node node;
    RedisReplyHelper redis_reply;
    struct ErrorInfo errinfo;
    CRedisNode* redis_node = NULL;

    if (key.empty() && cluster_mode())
    {
        // 集群模式必须指定key
        errinfo.errcode = ERROR_ZERO_KEY;
        errinfo.raw_errmsg = format_string("[%s] key is empty in cluster node", command_args.get_command());
        errinfo.errmsg = format_string("[%s:%d] %s", __FILE__, __LINE__, errinfo.raw_errmsg.c_str());
        (*g_error_log)("%s\n", errinfo.errmsg.c_str());
        THROW_REDIS_EXCEPTION(errinfo);
    }
    for (int loop_counter=0;;++loop_counter)
    {
        const int slot = cluster_mode()? get_key_slot(&key): -1;
        HandleResult errcode;
        redis_node = get_redis_node(slot, &errinfo);

        if (NULL == redis_node)
        {
            // 没有任何master
            break;
        }
        else if (NULL == redis_node->get_redis_context())
        {
            // 连接master不成功
            errcode = HR_RECONN_UNCOND;
        }
        else
        {
            if (which != NULL)
                *which = redis_node->get_node();
            if (_command_monitor != NULL)
                _command_monitor->before_execute(redis_node->get_node(), command_args.get_command_str(), command_args, readonly);

            redis_reply = (redisReply*)redisCommandArgv(
                    redis_node->get_redis_context(),
                    command_args.get_argc(), command_args.get_argv(), command_args.get_argvlen());
            if (!redis_reply)
                errcode = handle_redis_command_error(node, command_args, redis_node->get_redis_context(), &errinfo);
            else
                errcode = handle_redis_reply(node, command_args, redis_reply.get(), &errinfo);
        }

        if (HR_SUCCESS == errcode)
        {
            // 成功
            if (_command_monitor != NULL)
                _command_monitor->after_execute(0, redis_node->get_node(), command_args.get_command_str(), redis_reply.get());
            return redis_reply;
        }
        else if (HR_ERROR == errcode)
        {
            // 不需要重试的错误，比如：EVAL命令语法错误
            break;
        }
        else if (HR_RECONN_COND == errcode ||
                 HR_RECONN_UNCOND == errcode)
        {
            // 先调用close关闭连接，当调用get_redis_node时就会执行重连接
            redis_node->close();
        }

        // 决定是否重试
        if (HR_RECONN_UNCOND == errcode)
        {
            // 保持至少重试一次（前提是先重新建立好连接）
            if (loop_counter >= num_retries+1)
                break;
        }
        else if (HR_RETRY_UNCOND == errcode)
        {
            // 一般replica切换成master需要几秒钟，所以需要保证有足够的重试次数
            if (loop_counter >= NUM_RETRIES)
                break;
        }
        else if (loop_counter>=num_retries-1)
        {
            break;
        }

        // 控制重试频率，以增强重试成功率
        if (HR_RETRY_UNCOND == errcode)
        {
            const int retry_sleep_milliseconds = get_retry_sleep_milliseconds(loop_counter);
            if (retry_sleep_milliseconds > 0)
                millisleep(retry_sleep_milliseconds);
        }
        if (_command_monitor != NULL)
        {
            _command_monitor->after_execute(1, redis_node->get_node(), command_args.get_command_str(), redis_reply.get());
        }
    }

    // 错误以异常方式抛出
    if (_command_monitor != NULL)
        _command_monitor->after_execute(1, redis_node->get_node(), command_args.get_command_str(), redis_reply.get());
    THROW_REDIS_EXCEPTION_WITH_NODE_AND_COMMAND(
            errinfo, node.first, node.second,
            command_args.get_command_str(), command_args.get_key_str());
}

CRedisClient::HandleResult
CRedisClient::handle_redis_command_error(
        const Node& node,
        const CommandArgs& command_args,
        redisContext* redis_context,
        struct ErrorInfo* errinfo)
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
    const int redis_errcode = redis_context->err;
    errinfo->errcode = errno;
    errinfo->raw_errmsg = format_string("[%s:%d] (%d)%s",
            node.first.c_str(), node.second, redis_errcode, redis_context->errstr);
    errinfo->errmsg = format_string("[%s:%d][%s] %s",
            __FILE__, __LINE__, command_args.get_command(), errinfo->raw_errmsg.c_str());
    (*g_error_log)("%s\n", errinfo->errmsg.c_str());

    // REDIS_ERR_IO:
    // There was an I/O error while creating the connection, trying to write
    // to the socket or read from the socket. If you included `errno.h` in your
    // application, you can use the global `errno` variable to find out what is wrong.
    //
    // REDIS_ERR_EOF:
    // The server closed the connection which resulted in an empty read.
    //
    // REDIS_ERR_PROTOCOL:
    // There was an error while parsing the protocol.
    //
    // REDIS_ERR_OTHER:
    // Any other error. Currently, it is only used when a specified hostname to connect to cannot be resolved.
    if ((redis_errcode != REDIS_ERR_IO) &&
        (redis_errcode != REDIS_ERR_EOF))
    {
        return HR_ERROR; // Not retry
    }
    else if (REDIS_ERR_EOF == redis_errcode)
    {
        return HR_RECONN_COND; // Retry unconditionally and reconnect
    }
    else
    {
        return HR_RECONN_COND; // Retry conditionally
    }
}

CRedisClient::HandleResult
CRedisClient::handle_redis_reply(
        const Node& node,
        const CommandArgs& command_args,
        const redisReply* redis_reply,
        struct ErrorInfo* errinfo)
{
    if (redis_reply->type != REDIS_REPLY_ERROR)
        return HR_SUCCESS;
    else
        return handle_redis_replay_error(node, command_args, redis_reply, errinfo);
}

CRedisClient::HandleResult
CRedisClient::handle_redis_replay_error(
        const Node& node,
        const CommandArgs& command_args,
        const redisReply* redis_reply,
        struct ErrorInfo* errinfo)
{
    // ASK
    // NOAUTH Authentication required
    // MOVED 6474 127.0.0.1:6380
    // WRONGTYPE Operation against a key holding the wrong kind of value
    // CLUSTERDOWN The cluster is down
    // NOSCRIPT No matching script. Please use EVAL.
    extract_errtype(redis_reply, &errinfo->errtype);
    errinfo->errcode = ERROR_COMMAND;
    errinfo->raw_errmsg = format_string("[%s:%d] %s", node.first.c_str(), node.second, redis_reply->str);
    errinfo->errmsg = format_string("[%s:%d][%s] %s", __FILE__, __LINE__, command_args.get_command(), errinfo->raw_errmsg.c_str());
    (*g_error_log)("%s\n", errinfo->errmsg.c_str());

    // EVAL不会返回“MOVED”的错误，需特别处理
    if (is_clusterdown_error(errinfo->errtype))
    {
        return HR_RETRY_UNCOND;
    }
    else if (is_ask_error(errinfo->errtype))
    {
        return HR_RETRY_UNCOND;
    }
    else if (is_moved_error(errinfo->errtype))
    {
        //Node ask_node;
        //parse_moved_string(redis_reply->str, &ask_node);
        refresh_master_nodes(errinfo);
        return HR_RETRY_UNCOND;
    }
    else
    {
        return HR_ERROR;
    }
}

void CRedisClient::init()
{
    try
    {
        const int num_nodes = parse_nodes(&_nodes, _raw_nodes_string);
        struct ErrorInfo errinfo;

        if (0 == num_nodes)
        {
            errinfo.errcode = ERROR_PARAMETER;
            errinfo.errmsg = format_string("[%s:%d] parameter[nodes] error: %s", __FILE__, __LINE__, _raw_nodes_string.c_str());
            errinfo.raw_errmsg = format_string("parameter[nodes] error: %s", _raw_nodes_string.c_str());
            (*g_error_log)("%s\n", errinfo.errmsg.c_str());
            THROW_REDIS_EXCEPTION(errinfo);
        }
        else if (1 == num_nodes)
        {
            if (!init_standlone(&errinfo))
                THROW_REDIS_EXCEPTION(errinfo);
        }
        else
        {
            if (!init_cluster(&errinfo))
                THROW_REDIS_EXCEPTION(errinfo);
        }
    }
    catch (...)
    {
        clear_all_master_nodes();
        throw;
    }
}

bool CRedisClient::init_standlone(struct ErrorInfo* errinfo)
{
    const Node& node = _nodes[0];
    _master_nodes_string = _raw_nodes_string;

    redisContext* redis_context = connect_redis_node(node, errinfo);
    if (NULL == redis_context)
    {
        return false;
    }
    else
    {
        CMasterNode* redis_node = new CMasterNode(node, redis_context);
        const std::pair<MasterNodeTable::iterator, bool> ret =
                _master_nodes.insert(std::make_pair(node, redis_node));
        R3C_ASSERT(ret.second);
        return true;
    }
}

bool CRedisClient::init_cluster(struct ErrorInfo* errinfo)
{
    const int num_nodes = static_cast<int>(_nodes.size());
    uint64_t seed = reinterpret_cast<uint64_t>(this) - num_nodes;

    _slot2node.resize(CLUSTER_SLOTS);
    for (int i=0; i<num_nodes; ++i)
    {
        const int j = static_cast<int>(seed++ % num_nodes);
        const Node& node = _nodes[j];
        std::vector<struct NodeInfo> nodes_info;

        redisContext* redis_context = connect_redis_node(node, errinfo);
        if (NULL == redis_context)
        {
            continue;
        }
        if (!list_cluster_nodes(&nodes_info, errinfo, redis_context, node))
        {
            redisFree(redis_context);
            continue;
        }
        else
        {
            redisFree(redis_context);
            redis_context = NULL;

            // MASTER
            if (init_master_nodes(nodes_info, errinfo))
            {
                break;
            }
        }
    }

    return !_master_nodes.empty();
}

bool CRedisClient::init_master_nodes(const std::vector<struct NodeInfo>& nodes_info, struct ErrorInfo* errinfo)
{
    int num = 0;

    for (std::vector<struct NodeInfo>::size_type i=0; i<nodes_info.size(); ++i)
    {
        const struct NodeInfo& nodeinfo = nodes_info[i];

        if (nodeinfo.is_master())
        {
            update_slots(nodeinfo);
            update_master_nodes_string(nodeinfo);

            if (add_master_node(nodeinfo, errinfo))
                ++num;
        }
    }

    // 至少要有一个能够连接上
    return num > 0;
}

void CRedisClient::update_slots(const struct NodeInfo& nodeinfo)
{
    for (SlotSegment::size_type i=0; i<nodeinfo.slots.size(); ++i)
    {
        const std::pair<int, int>& slot_segment = nodeinfo.slots[i];
        for (int slot=slot_segment.first; slot<slot_segment.second; ++slot)
            _slot2node[slot] = nodeinfo.node;
    }
}

void CRedisClient::refresh_master_nodes(struct ErrorInfo* errinfo)
{
    const int num_nodes = static_cast<int>(_master_nodes.size());
    uint64_t seed = reinterpret_cast<uint64_t>(this) - num_nodes;
    const int k = static_cast<int>(seed % num_nodes);
    MasterNodeTable::iterator iter = _master_nodes.begin();

    for (int i=0; i<k; ++i)
    {
        ++iter;
        if (iter == _master_nodes.end())
            iter = _master_nodes.begin();
    }
    for (int i=0; i<num_nodes; ++i)
    {
        const Node& node = iter->first;
        CMasterNode* redis_node = iter->second;
        redisContext* redis_context = redis_node->get_redis_context();

        if (NULL == redis_context)
        {
            redis_context = connect_redis_node(node, errinfo);
        }
        if (redis_context != NULL)
        {
            std::vector<struct NodeInfo> nodes_info;

            redis_node->set_redis_context(redis_context);
            if (list_cluster_nodes(&nodes_info, errinfo, redis_context, node))
            {
                clear_and_update_master_nodes(nodes_info, errinfo);
                break; // Continue is not safe, because `clear_and_update_master_nodes` will modify _master_nodes
            }
        }
        if (iter == _master_nodes.end())
        {
            iter = _master_nodes.begin();
        }
    }
}

void CRedisClient::clear_and_update_master_nodes(const std::vector<struct NodeInfo>& nodes_info, struct ErrorInfo* errinfo)
{
    NodeInfoTable node_info_table;

    _master_nodes_string.clear();
    for (std::vector<struct NodeInfo>::size_type i=0; i<nodes_info.size(); ++i)
    {
        const struct NodeInfo& nodeinfo = nodes_info[i];

        if (!nodeinfo.is_master())
        {
            node_info_table.insert(std::make_pair(nodeinfo.node, nodeinfo));
        }
        else
        {
            // 可能只是一个或多个slot从一个master迁到另一个master，
            // 简单的全量更新slot和node间的关系，
            // 如果一对master和replica同时异常，则_slot2node会出现空洞
            update_slots(nodeinfo);
            update_master_nodes_string(nodeinfo);

            if (_master_nodes.count(nodeinfo.node) <= 0)
            {
                // New master
                add_master_node(nodeinfo, errinfo);
            }
        }
    }

    clear_invalid_master_nodes(node_info_table);
}

void CRedisClient::clear_invalid_master_nodes(const NodeInfoTable& node_info_table)
{
    for (MasterNodeTable::iterator iter=_master_nodes.begin(); iter!=_master_nodes.end();)
    {
        const Node& node = iter->first;
        const NodeInfoTable::const_iterator info_iter = node_info_table.find(node);

        if (info_iter != node_info_table.end())
        {
            const struct NodeInfo& nodeinfo = info_iter->second;

            if (nodeinfo.is_master())
            {
                ++iter;
            }
            else
            {
                // 极端情况下，可能将所有的master都干掉了在，
                // 而且可能原来参数传入的所有nodes都可能被删除了，
                // 这个时候只有考虑从_master_nodes_string中恢复，
                // 但是正常不应当出现，万一出现只需简单抛出异常终止执行即可，亦即要求时刻至少有一个可用的master。
                CMasterNode* master_node = iter->second;
                (*g_info_log)("%s deleted because it is not master now\n", master_node->str().c_str());
                iter = _master_nodes.erase(iter);
                delete master_node;
            }
        }
    }
}

bool CRedisClient::add_master_node(const NodeInfo& nodeinfo, struct ErrorInfo* errinfo)
{
    redisContext* redis_context = connect_redis_node(nodeinfo.node, errinfo);
    CMasterNode* master_node = new CMasterNode(nodeinfo.node, redis_context);

    const std::pair<MasterNodeTable::iterator, bool> ret =
            _master_nodes.insert(std::make_pair(nodeinfo.node, master_node));
    R3C_ASSERT(ret.second);
    if (!ret.second)
        delete master_node;
    return redis_context != NULL;
}

void CRedisClient::clear_all_master_nodes()
{
    for (MasterNodeTable::iterator iter=_master_nodes.begin(); iter!=_master_nodes.end(); ++iter)
    {
        CMasterNode* master_node = iter->second;
        delete master_node;
    }
    _master_nodes.clear();
}

void CRedisClient::update_master_nodes_string(const NodeInfo& nodeinfo)
{
    std::string node_str;
    if (_master_nodes_string.empty())
        _master_nodes_string = node2string(nodeinfo.node, &node_str);
    else
        _master_nodes_string = _master_nodes_string + std::string(",") + node2string(nodeinfo.node, &node_str);
}

redisContext* CRedisClient::connect_redis_node(const Node& node, struct ErrorInfo* errinfo) const
{
    redisContext* redis_context = NULL;

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
        errinfo->raw_errmsg = "can not allocate redis context";
        errinfo->errmsg = format_string("[%s:%d][%s:%d] %s",
                __FILE__, __LINE__, node.first.c_str(), node.second, errinfo->raw_errmsg.c_str());
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
        {
            errinfo->errmsg = format_string("[%s:%d][%s:%d] (errno:%d,err:%d)%s",
                    __FILE__, __LINE__, node.first.c_str(), node.second, errno, redis_context->err, errinfo->raw_errmsg.c_str());
        }
        else
        {
            errinfo->errmsg = format_string("[%s:%d][%s:%d] (err:%d)%s",
                    __FILE__, __LINE__, node.first.c_str(), node.second, redis_context->err, errinfo->raw_errmsg.c_str());
        }
        (*g_error_log)("%s\n", errinfo->errmsg.c_str());
        redisFree(redis_context);
        redis_context = NULL;
    }
    else
    {
        if (_readwrite_timeout_milliseconds > 0)
        {
            struct timeval data_timeout;
            data_timeout.tv_sec = _readwrite_timeout_milliseconds / 1000;
            data_timeout.tv_usec = (_readwrite_timeout_milliseconds % 1000) * 1000;

            if (REDIS_ERR == redisSetTimeout(redis_context, data_timeout))
            {
                // REDIS_ERR_IO == redis_context->err
                errinfo->errcode = ERROR_INIT_REDIS_CONN;
                errinfo->raw_errmsg = redis_context->errstr;
                errinfo->errmsg = format_string("[%s:%d][%s:%d] (errno:%d,err:%d)%s",
                        __FILE__, __LINE__, node.first.c_str(), node.second, errno, redis_context->err, errinfo->raw_errmsg.c_str());
                (*g_error_log)("%s\n", errinfo->errmsg.c_str());
                redisFree(redis_context);
                redis_context = NULL;
            }
        }

        if ((0 == errinfo->errcode) && !_password.empty())
        {
            const RedisReplyHelper redis_reply = (redisReply*)redisCommand(redis_context, "AUTH %s", _password.c_str());

            if (redis_reply && 0==strcmp(redis_reply->str,"OK"))
            {
                // AUTH success
                (*g_info_log)("[%s:%d] connect redis://%s:%d success\n", __FILE__, __LINE__, node.first.c_str(), node.second);
            }
            else
            {
                // AUTH failed
                if (!redis_reply)
                {
                    errinfo->errcode = ERROR_REDIS_AUTH;
                    errinfo->raw_errmsg = "authorization failed";
                }
                else
                {
                    extract_errtype(redis_reply.get(), &errinfo->errtype);
                    errinfo->errcode = ERROR_REDIS_AUTH;
                    errinfo->raw_errmsg = redis_reply->str;
                }

                errinfo->errmsg = format_string("[%s:%d][%s:%d] %s",
                        __FILE__, __LINE__, node.first.c_str(), node.second, errinfo->raw_errmsg.c_str());
                (*g_error_log)("%s\n", errinfo->errmsg.c_str());
                redisFree(redis_context);
                redis_context = NULL;
            }
        }
    }

    return redis_context;
}

CRedisNode* CRedisClient::get_redis_node(int slot, struct ErrorInfo* errinfo) const
{
    CRedisNode* redis_node = NULL;
    redisContext* redis_context = NULL;

    if (-1 == slot)
    {
        // Standalone
        R3C_ASSERT(!_master_nodes.empty());
        redis_node = _master_nodes.begin()->second;
    }
    else
    {
        // Cluster
        R3C_ASSERT(slot>=0 && CLUSTER_SLOTS<=CLUSTER_SLOTS);

        const Node& node = _slot2node[slot];
        MasterNodeTable::const_iterator iter = _master_nodes.find(node);
        if (iter != _master_nodes.end())
        {
            redis_node = iter->second;
        }
    }

    if (NULL == redis_node)
    {
        // 遇到空的slot，随机选一个
        redis_node = random_master_node();
    }
    else
    {
        redis_context = redis_node->get_redis_context();
        if (NULL == redis_context)
        {
            redis_context = connect_redis_node(redis_node->get_node(), errinfo);
            redis_node->set_redis_context(redis_context);
        }
    }
    return redis_node;
}

CMasterNode* CRedisClient::random_master_node() const
{
    if (_master_nodes.empty())
    {
        return NULL;
    }
    else
    {
        const int num_nodes = static_cast<int>(_nodes.size());
        uint64_t seed = reinterpret_cast<uint64_t>(this) - num_nodes;
        const int k = static_cast<int>(seed % num_nodes);
        MasterNodeTable::const_iterator iter = _master_nodes.begin();

        for (int i=0; i<k; ++i)
        {
            ++iter;
            if (iter == _master_nodes.end())
                iter = _master_nodes.begin();
        }
        return iter->second;
    }
}

bool
CRedisClient::list_cluster_nodes(
        std::vector<struct NodeInfo>* nodes_info,
        struct ErrorInfo* errinfo,
        redisContext* redis_context,
        const Node& node)
{
    const RedisReplyHelper redis_reply = (redisReply*)redisCommand(redis_context, "CLUSTER NODES");

    errinfo->clear();
    if (!redis_reply)
    {
        errinfo->errcode = ERROR_COMMAND;
        errinfo->raw_errmsg = "redisCommand failed";
        errinfo->errmsg = format_string("[%s:%d] %s", __FILE__, __LINE__, "redisCommand failed");
        (*g_error_log)("%s\n", errinfo->errmsg.c_str());
    }
    else if (REDIS_REPLY_ERROR == redis_reply->type)
    {
        // ERR This instance has cluster support disabled
        errinfo->errcode = ERROR_COMMAND;
        errinfo->raw_errmsg = redis_reply->str;
        errinfo->errmsg = format_string("[%s:%d] %s", __FILE__, __LINE__, redis_reply->str);
        (*g_error_log)("%s\n", errinfo->errmsg.c_str());
    }
    else if (redis_reply->type != REDIS_REPLY_STRING)
    {
        // Unexpected reply type
        errinfo->errcode = ERROR_UNEXCEPTED_REPLY_TYPE;
        errinfo->raw_errmsg = redis_reply->str;
        errinfo->errmsg = format_string("[%s:%d] (type:%d)%s", __FILE__, __LINE__, redis_reply->type, redis_reply->str);
        (*g_error_log)("%s\n", errinfo->errmsg.c_str());
    }
    else
    {
        /*
         * <id> <ip:port> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
         *
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
            errinfo->raw_errmsg = "reply nothing";
            errinfo->errmsg = format_string("[%s:%d] %s", __FILE__, __LINE__, "reply nothing");
            (*g_error_log)("%s\n", errinfo->errmsg.c_str());
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
                nodes_info->clear();
                errinfo->errcode = ERROR_REPLY_FORMAT;
                errinfo->raw_errmsg = "reply format error";
                errinfo->errmsg = format_string("[%s:%d] %s", __FILE__, __LINE__, "reply format error");
                (*g_error_log)("%s\n", errinfo->errmsg.c_str());
                break;
            }
            else
            {
                NodeInfo node_info;
                node_info.id = tokens[0];

                if (!parse_node_string(tokens[1], &node_info.node.first, &node_info.node.second))
                {
                    nodes_info->clear();
                    errinfo->errcode = ERROR_REPLY_FORMAT;
                    errinfo->raw_errmsg = "reply format error";
                    errinfo->errmsg = format_string("[%s:%d] %s", __FILE__, __LINE__, "reply format error");
                    (*g_error_log)("%s\n", errinfo->errmsg.c_str());
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

                    (*g_debug_log)("[%s:%d][%s:%d] %s\n",
                            __FILE__, __LINE__, node.first.c_str(), node.second, node_info.str().c_str());
                    nodes_info->push_back(node_info);
                }
            }
        }
    }

    return !nodes_info->empty();
}

// Extract error type, such as ERR, MOVED, WRONGTYPE, ...
void CRedisClient::extract_errtype(const redisReply* redis_reply, std::string* errtype) const
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

int
CRedisClient::get_values(
        const redisReply* redis_reply,
        const std::vector<std::string>& fields,
        bool keep_null,
        std::map<std::string, std::string>* map)
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
