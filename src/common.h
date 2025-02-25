#pragma once

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <vector>
#include <cstdint>
#include <unordered_set>
#include <unordered_map>
#include <type_traits>
#include <array>
#include <csignal>
#include <thread>
#include <atomic>

inline void convertBE16toH(int16_t &first)
{
    first = be16toh(first);
}

inline void convertBE16toH(int16_t &first, auto &...rest)
{
    first = be16toh(first);
    convertBE16toH(rest...);
}

inline void convertBE32toH(int32_t &first)
{
    first = be32toh(first);
}

inline void convertBE32toH(int32_t &first, auto &...rest)
{
    first = be32toh(first);
    convertBE32toH(rest...);
}

inline void convertH16toBE(int16_t &first)
{
    first = htobe16(first);
}

inline void convertH16toBE(int16_t &first, auto &...rest)
{
    first = htobe16(first);
    convertH16toBE(rest...);
}

inline void convertH32toBE(int32_t &first)
{
    first = htobe32(first);
}

inline void convertH32toBE(int32_t &first, auto &...rest)
{
    first = htobe32(first);
    convertH32toBE(rest...);
}

extern std::atomic_bool server_running;