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
            _current = null;
        }
        public IBatchContainer GetCurrent(out Exception exception)
        {
            try
            {
                exception = null;
                _logger.AutoVerbose($"Returning {_current?.Value}, with token {_current?.Value.SequenceToken}");
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
            try
            {
                // if this is the first time
                if (_current == null)
                {
                    _logger.AutoVerbose("_current is null, must be first time calling MoveNext or cache is empty");
                    _logger.AutoVerbose($"_cache is null: {_cache == null} , and has {_cache?.Count} messages");
                    if (_cache.First != null)
                    {
                        _logger.AutoVerbose("Setting _current to first message in cache");
                        _current = _cache.First;
                        _logger.AutoVerbose("set _current to first message in cache");

                        if (_current.Value?.StreamNamespace == _namespace && _current.Value?.StreamGuid == _stream)
                        {
                            _logger.AutoVerbose("first message in cache is for this stream, successfully moved to next. Returning true");
                            return true;
                        }
                        else
                        {
                            _logger.AutoVerbose($"First message is NOT for this stream. This stream: {_namespace}-{_stream}, first message is for stream: {_current.Value?.StreamNamespace}-{_current.Value?.StreamGuid}");
                        }
                    }
                    else
                    {
                        _logger.AutoVerbose("Cache is empty");
                        return false;
                    }
                }

                while (true)
                {
                    if (_current.Next == null) // end?
                    {
                        _logger.AutoVerbose("no next, returning false");
                        return false;
                    }

                    _logger.AutoVerbose("advancing to next");
                    _current = _current.Next;
                    _logger.AutoVerbose("advanced to next");
                    // Find batch with the same token, no this namespace and for this stream
                    if (_current.Value?.StreamNamespace == _namespace && _current.Value?.StreamGuid == _stream)
                    {
                        _logger.AutoVerbose("this message is for this stream, returning true");
                        return true;
                    }
                    else {
                        _logger.AutoVerbose($"this message is NOT for this stream. This stream: {_namespace}-{_stream}, this message is for stream: {_current.Value?.StreamNamespace}-{_current.Value?.StreamGuid}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.AutoError($"{ex}");
                return false;
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