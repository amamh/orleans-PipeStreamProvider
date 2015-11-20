﻿using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PipeStreamProvider.RedisCache
{
    public class QueueCacheRedisCursor : IQueueCacheCursor
    {
        private readonly RedisCustomList<IBatchContainer> _cache;
        private int _index = 0;

        private readonly string _namespace;
        private readonly Guid _stream;
        private StreamSequenceToken _token;

        public QueueCacheRedisCursor(RedisCustomList<IBatchContainer> cache, string streamNamespace, Guid streamGuid, StreamSequenceToken token)
        {
            _cache = cache;
            _namespace = streamNamespace;
            _stream = streamGuid;
            _token = token;
        }

        public IBatchContainer GetCurrent(out Exception exception)
        {
            try
            {
                exception = null;
                var batch = _cache.Get(_index);
                return batch;
            }
            catch (Exception ex)
            {
                exception = ex;
                throw;
            }
        }

        public bool MoveNext()
        {
            IBatchContainer next;
            while (_index < _cache.Count)
            {
                _index++;
                next = _cache.Get(_index); // TODO: we will do this retrieval again laster in GetCurrent, maybe we can cache it

                if (next?.StreamNamespace == _namespace && next?.StreamGuid == _stream)
                    return true;
            }
            return false;
        }

        public void Refresh()
        {
            // nothing to do here
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~QueueCacheRedisCursor() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}