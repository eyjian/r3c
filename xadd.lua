-- Batch xadd one by one
--
-- Usage:
-- redis-cli --no-auth-warning -a PASSWORD -h HOST -p PORT -c --eval ./xadd.lua k , 10 2 f1 v1 f2 v2 f3 v3 f4 v4 f5 v5
--
-- Compile xadd.lua to C++ code (xadd_lua &xadd_lua_len):
-- xxd -i xadd.lua xadd.cpp
--
-- KEYS[1] key of stream
-- ARGV[1] maxlen
-- ARGV[2] count
-- ARGV[3] field
-- ARGV[4] value
-- ARGV[5] field
-- ARGV[6] value
local key=KEYS[1]
local maxlen=ARGV[1]
local count=ARGV[2]

-- table.remove(ARGV,1)
-- redis.call('XADD',key,'MAXLEN','~',maxlen,'*',unpack(ARGV))
for i=3,#ARGV,2 do
	local field=ARGV[i]
	local value=ARGV[i+1]
	redis.call('XADD',key,'MAXLEN','~',maxlen,'*',field,value)
end
if tonumber(count)>0 then
	return redis.call('XRANGE',key,'-','+','COUNT',count)
end
return nil

--[[

extern unsigned char xadd_lua[];
extern unsigned int xadd_lua_len;

inline void xadd(
	CRedisClient* redis, const std::string& key, 
	int64_t maxlen, int64_t count, 
	const std::vector<FVPair>& fvpairs, std::vector<StreamEntry>* values,
	Node* which=NULL, int num_retries=0)
{
	static std::string xadd_lua_script(reinterpret_cast<char*>(xadd_lua), static_cast<std::string::size_type>(xadd_lua_len));
	std::vector<std::string> parameters;

	parameters.emplace_back(int2string(maxlen));
	parameters.emplace_back(int2string(count));
	for (auto& fvpair: fvpairs)
	{
		parameters.emplace_back(fvpair.field);
		parameters.emplace_back(fvpair.value);
	}
	const RedisReplyHelper redis_reply = redis->eval(key, xadd_lua_script, parameters, which, num_retries);
	if (redis_reply->type != REDIS_REPLY_NIL)
		CRedisClient::get_values(redis_reply.get(), values);
}

--]]
