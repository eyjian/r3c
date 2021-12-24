// Writed by yijian (eyjian@qq.com)
#include <r3c/r3c.h>
#include <string>
#include <vector>
namespace r3c {

// Batch to lpop
inline int lpop(CRedisClient* redis, const std::string& key, std::vector<std::string>* values, int n, Node* which=NULL, int num_retries=0)
{
    values->clear();

    // LRANGE key start stop
    // Offsets start and stop are zero-based indexes,
    // with 0 being the first element of the list (the head of the list),
    // 1 being the next element and so on.
    //
    // LTRIM key start stop
    // Both start and stop are zero-based indexes,
    // where 0 is the first element of the list (the head),
    // 1 the next element and so on.
    const std::string lua_scripts =
            "local v=redis.call('LRANGE',KEYS[1],0,ARGV[1]-1);"
            "redis.call('LTRIM',KEYS[1],ARGV[1],-1);"
            "return v;";
    std::vector<std::string> parameters(1);
    if (n > 0)
        parameters[0] = int2string(n);
    else
        parameters[0] = "1";
    const RedisReplyHelper redis_reply = redis->eval(key, lua_scripts, parameters, which, num_retries);
    if (redis_reply->type == REDIS_REPLY_ARRAY)
        return CRedisClient::get_values(redis_reply.get(), values);
    return 0;
}

inline bool hsetex(CRedisClient* redis, const std::string& key, const std::string& field, const std::string& value, uint32_t expired_seconds, Node* which=NULL, int num_retries=0)
{
    const std::string lua_scripts =
            "local n;n=redis.call('HSET',KEYS[1],ARGV[1],ARGV[2]);"
            "if (n>0) then redis.call('EXPIRE',KEYS[1],ARGV[3]) end;return n;";
    std::vector<std::string> parameters(3);
    parameters[0] = field;
    parameters[1] = value;
    parameters[2] = int2string(expired_seconds);
    const RedisReplyHelper redis_reply = redis->eval(key, lua_scripts, parameters, which, num_retries);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

inline bool hsetnxex(CRedisClient* redis, const std::string& key, const std::string& field, const std::string& value, uint32_t expired_seconds, Node* which=NULL, int num_retries=0)
{
    const std::string lua_scripts =
            "local n=redis.call('HLEN',KEYS[1]);"
            "local m=redis.call('HSETNX',KEYS[1],ARGV[1],ARGV[2]);"
            "if(n==0) then redis.call('EXPIRE',KEYS[1],ARGV[3]) end;return m;";
    std::vector<std::string> parameters(3);
    parameters[0] = field;
    parameters[1] = value;
    parameters[2] = int2string(expired_seconds);
    const RedisReplyHelper redis_reply = redis->eval(key, lua_scripts, parameters, which, num_retries);
    if (REDIS_REPLY_INTEGER == redis_reply->type)
        return 1 == redis_reply->integer;
    return true;
}

inline void hmincrby(CRedisClient* redis, const std::string& key, const std::vector<std::pair<std::string, int64_t> >& increments, std::vector<int64_t>* newvalues, Node* which=NULL, int num_retries=0)
{
    const std::string lua_scripts =
            "local j=1;local results={};"
            "for i=1,#ARGV,2 do local f=ARGV[i];"
            "local v=ARGV[i+1];"
            "results[j]=redis.call('HINCRBY',KEYS[1],f,v);j=j+1; end;"
            "return results;";
    std::vector<std::string> parameters(2*increments.size());
    for (std::vector<std::pair<std::string, int64_t> >::size_type i=0,j=0; i<increments.size(); ++i,j+=2)
    {
        const std::pair<std::string, int64_t>& increment = increments[i];
        parameters[j] = increment.first;
        parameters[j+1] = int2string(increment.second);
    }
    const RedisReplyHelper redis_reply = redis->eval(key, lua_scripts, parameters, which, num_retries);
    if (REDIS_REPLY_ARRAY == redis_reply->type)
        get_values(redis_reply.get(), newvalues);
}

} // namespace r3c {
