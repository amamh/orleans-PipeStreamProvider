using System;
using System.Collections.Generic;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using System.Diagnostics;

// TODO: Scrap this and do it from scratch without all this complexity
namespace PipeStreamProvider.MemoryCache
{
    public class QueueCacheCursor : IQueueCacheCursor
    {
        private StreamSequenceToken OldestPossibleToken { get; } = new SimpleSequenceToken(0);

        private readonly LinkedList<IBatchContainer> _cache;
        private readonly string _namespace;
        private readonly Guid _stream;
        private StreamSequenceToken _requestedToken;
        private LinkedListNode<IBatchContainer> _current;

        public QueueCacheCursor(LinkedList<IBatchContainer> cache, string streamNamespace, Guid streamGuid, SimpleSequenceToken token, Logger logger)
        {
            if (token != null && token.Older(OldestPossibleToken))
                throw new QueueCacheMissException($"Can't ask for a token older than SimpleSequenceToken(0). Requested token:\n{token}");

            _cache = cache;
            _namespace = streamNamespace;
            _stream = streamGuid;
            _requestedToken = token ?? OldestPossibleToken;
            _logger = logger;
            _current = _cache.First;
        }
        public IBatchContainer GetCurrent(out Exception exception)
        {
            try
            {
                exception = null;
                return _current.Value;
            }
            catch (Exception ex)
            {
                exception = ex;
                throw;
            }
        }

        public bool MoveNext()
        {
            if (_current == null)
            {
                if (_cache.First != null)
                {
                    _current = _cache.First;
                    return true;
                }
                else
                    return false;
            }

            var next = _current.Next;
            while (true)
            {
                if (next == null) // end?
                    return false;

                // Find batch with the same token, no this namespace and for this stream
                if (
                    next.Value?.StreamNamespace == _namespace
                    && next.Value?.StreamGuid == _stream
                    )
                {
                    _current = next;
                    return true;
                }

                next = next.Next;
            }
        }

        public void Refresh()
        {
            // nothing to do here
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls
        private Logger _logger;

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