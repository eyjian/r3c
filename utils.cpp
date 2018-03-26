#include "utils.h"
#include "sha1.h"
#include <hiredis/hiredis.h>
#include <ostream>
#include <poll.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

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

void null_log_write(const char* UNUSED(format), ...)
{
}

void r3c_log_write(const char* format, ...)
{
    va_list ap;
    va_start(ap, format);
    vprintf(format, ap);
    va_end(ap);
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

/* Copy from crc16.cpp
 *
 * CRC16 implementation according to CCITT standards.
 *
 * Note by @antirez: this is actually the XMODEM CRC 16 algorithm, using the
 * following parameters:
 *
 * Name                       : "XMODEM", also known as "ZMODEM", "CRC-16/ACORN"
 * Width                      : 16 bit
 * Poly                       : 1021 (That is actually x^16 + x^12 + x^5 + 1)
 * Initialization             : 0000
 * Reflect Input byte         : False
 * Reflect Output CRC         : False
 * Xor constant to output CRC : 0000
 * Output for "123456789"     : 31C3
 */

static const uint16_t crc16tab[256]= {
    0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
    0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
    0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
    0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
    0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
    0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
    0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
    0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
    0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
    0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
    0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
    0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
    0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
    0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
    0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
    0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
    0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
    0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
    0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
    0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
    0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
    0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
    0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
    0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
    0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
    0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
    0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
    0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
    0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
    0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
    0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
    0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

uint16_t crc16(const char *buf, int len) {
    int counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
            crc = (crc<<8) ^ crc16tab[((crc>>8) ^ *buf++)&0x00FF];
    return crc;
}

/* Copy from cluster.c
 *
 * We have 16384 hash slots. The hash slot of a given key is obtained
 * as the least significant 14 bits of the crc16 of the key.
 *
 * However if the key contains the {...} pattern, only the part between
 * { and } is hashed. This may be useful in the future to force certain
 * keys to be in the same node (assuming no resharding is in progress).
 */
int keyHashSlot(const char *key, size_t keylen) {
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

int get_key_slot(const std::string* key) {
    if ((key != NULL) && !key->empty())
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

void millisleep(int milliseconds)
{
#if SLEEP_USE_POLL==1
    poll(NULL, 0, milliseconds); // 可能被中断提前结束
#else
    struct timespec ts = { milliseconds / 1000, (milliseconds % 1000) * 1000000 };
    while ((-1 == nanosleep(&ts, &ts)) && (EINTR == errno));
#endif
}

std::string format_string(const char* format, ...) {
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

int parse_nodes(std::vector<std::pair<std::string, uint16_t> >* nodes, const std::string& nodes_string)
{
    std::string::size_type len = 0;
    std::string::size_type pos = 0;
    std::string::size_type comma_pos = 0;

    while (comma_pos != std::string::npos)
    {
        comma_pos = nodes_string.find(',', pos);
        if (comma_pos != std::string::npos)
            len = comma_pos - pos;
        else
            len = nodes_string.size() - comma_pos;

        if (len > 0)
        {
            const std::string& str = nodes_string.substr(pos, len);
            const std::string::size_type colon_pos = str.find(':');
            if (colon_pos != std::string::npos)
            {
                const std::string& ip_str = str.substr(0, colon_pos);
                const std::string& port_str = str.substr(colon_pos + 1);
                nodes->push_back(std::make_pair(ip_str, (uint16_t)atoi(port_str.c_str())));
            }
        }

        pos = comma_pos + 1; // Next node
    }

    return static_cast<int>(nodes->size());
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

bool parse_node_string(const std::string& node_string, std::string* ip, uint16_t* port)
{
    const std::string::size_type colon_pos = node_string.find(':');

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

void parse_slot_string(const std::string& slot_string, int* start_slot, int* end_slot)
{
    const std::string::size_type bar_pos = slot_string.find('-');

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

// MOVED 9166 10.240.84.140:6379
void parse_moved_string(const std::string& moved_string, std::pair<std::string, uint16_t>* node)
{
    const std::string::size_type space_pos = moved_string.rfind(' ');
    const std::string& ip_and_port_string = moved_string.substr(space_pos+1);
    const std::string::size_type colon_pos = ip_and_port_string.find(':');
    node->first = ip_and_port_string.substr(0, colon_pos);
    node->second = (uint16_t)atoi(ip_and_port_string.substr(colon_pos+1).c_str());
}

} // namespace r3c {
