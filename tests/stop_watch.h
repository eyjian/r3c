// Writed by yijian on 2015/6/17
// 秒表用于计时
#ifndef R3C_TESTS_STOP_WATCH_H
#define R3C_TESTS_STOP_WATCH_H
#include <stdlib.h>
#include <stdint.h>
#include <sys/time.h>

class CStopWatch;
typedef void (*StopWatchTick)(CStopWatch*, void*);

// 计时器
class CStopWatch
{
public:
    CStopWatch(StopWatchTick tick=NULL, void* tick_data=NULL)
            : _tick(tick), _tick_data(tick_data)
    {
        restart();

        _stop_time.tv_sec = 0;
        _stop_time.tv_usec = 0;
        _total_time.tv_sec = _start_time.tv_sec;
        _total_time.tv_usec = _start_time.tv_usec;
    }

    ~CStopWatch()
    {
        if (_tick != NULL)
            (*_tick)(this, _tick_data);
    }

    // 重新开始计时
    void restart()
    {
        (void)gettimeofday(&_start_time, NULL);
    }

    // 返回微秒级的耗时
    // restart 调用之后是否重新开始计时
    uint64_t get_elapsed_microseconds(bool restart=true)
    {
        (void)gettimeofday(&_stop_time, NULL);
        const int64_t elapsed_microseconds = static_cast<int64_t>((_stop_time.tv_sec - _start_time.tv_sec) * (__UINT64_C(1000000)) + (_stop_time.tv_usec - _start_time.tv_usec));

        // 重计时
        if (restart)
        {
            _start_time.tv_sec = _stop_time.tv_sec;
            _start_time.tv_usec = _stop_time.tv_usec;
        }
        if (elapsed_microseconds < 0)
            return 0;
        else
            return static_cast<uint64_t>(elapsed_microseconds);
    }

    uint64_t get_total_elapsed_microseconds()
    {
        (void)gettimeofday(&_stop_time, NULL);
        const uint64_t total_elapsed_microseconds = static_cast<uint64_t>((_stop_time.tv_sec - _total_time.tv_sec) * (__UINT64_C(1000000)) + (_stop_time.tv_usec - _total_time.tv_usec));
        return total_elapsed_microseconds;
    }

    // 相当于time(NULL)
    // 得到构造时系统的当前时间
    time_t get_start_seconds() const
    {
        return _total_time.tv_sec;
    }

private:
    StopWatchTick _tick;
    void* _tick_data;
    struct timeval _total_time;
    struct timeval _start_time;
    struct timeval _stop_time;
};

#endif // R3C_TESTS_STOP_WATCH_H