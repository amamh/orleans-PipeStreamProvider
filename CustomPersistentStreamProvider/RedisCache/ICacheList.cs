using System;
using Orleans.Streams;

namespace PipeStreamProvider.RedisCache
{
    public interface ICacheList<T>
    {
        T Get(int index);
        T LeftPop();
        bool LeftPush(T v);
        T RightPop();
        bool RightPush(T v);
        bool Set(int index, T newVal);

        int Count { get; }
    }
}