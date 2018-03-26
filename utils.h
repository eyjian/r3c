#ifndef REDIS_CLUSTER_CLIENT_UTILS_H
#define REDIS_CLUSTER_CLIENT_UTILS_H
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <string.h>
#include <sys/time.h>
#include <vector>
#include <unistd.h>

#ifdef __GNUC__
#  define UNUSED(x) UNUSED_ ## x __attribute__((__unused__))
#else
#  define UNUSED(x) UNUSED_ ## x
#endif

std::ostream& operator <<(std::ostream& os, const struct redisReply& redis_reply);

namespace r3c {

extern void null_log_write(const char* UNUSED(format), ...); // Discard log
extern void r3c_log_write(const char* format, ...); // Ouput log to stdout
extern std::string strsha1(const std::string& str);
extern uint16_t crc16(const char *buf, int len);
extern int keyHashSlot(const char *key, size_t keylen);
extern int parse_nodes(std::vector<std::pair<std::string, uint16_t> >* nodes, const std::string& nodes_string);
extern bool parse_node_string(const std::string& node_string, std::string* ip, uint16_t* port);
extern void parse_slot_string(const std::string& slot_string, int* start_slot, int* end_slot);
extern void parse_moved_string(const std::string& moved_string, std::pair<std::string, uint16_t>* node);

} // namespace r3c {
#endif // REDIS_CLUSTER_CLIENT_UTILS_H
