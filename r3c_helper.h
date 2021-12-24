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

} // namespace r3c {
