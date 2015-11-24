using System;
using Orleans.Streams;

namespace PipeStreamProvider.RedisCache
{
    public interface ICacheList<T>
    {
        T Get(long index);
        T LeftPop();
        bool LeftPush(T v);
        T RightPop();
        bool RightPush(T v);
        bool Set(long index, T newVal);

        long Count { get; }
    }
}