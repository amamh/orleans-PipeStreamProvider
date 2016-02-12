using System;
using System.Collections.Generic;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using System.Diagnostics;

namespace PipeStreamProvider.Cache
{
    public class QueueCacheCursor : IQueueCacheCursor
    {
        //private TimeSequenceToken OldestPossibleToken { get; } = new SimpleSequenceToken(0);

        private readonly LinkedList<PipeQueueAdapterBatchContainer> _cache;
        private readonly string _namespace;
        private readonly Guid _stream;
        private TimeSequenceToken _requestedToken;
        private LinkedListNode<PipeQueueAdapterBatchContainer> _current;
        /// <summary>
        /// What the cursor is currently on
        /// </summary>
        public TimeSequenceToken CurrentToken => _current?.Value.RealToken;
        public bool IsSet { get; set; }

        public QueueCacheCursor(LinkedList<PipeQueueAdapterBatchContainer> cache, string streamNamespace, Guid streamGuid, TimeSequenceToken token, Logger logger)
        {
            //if (token != null && token.Older(OldestPossibleToken))
            //    throw new QueueCacheMissException($"Can't ask for a token older than SimpleSequenceToken(0). Requested token:\n{token}");

            _cache = cache;
            _namespace = streamNamespace;
            _stream = streamGuid;
            _requestedToken = token;
            _logger = logger;
            _current = null;
        }

        public IBatchContainer GetCurrent(out Exception exception)
        {
            exception = null;
            _logger.AutoVerbose($"Returning {_current?.Value}, with token {_current?.Value.SequenceToken}");
            return _current?.Value;
        }

        public bool MoveNext()
        {
            // Client is asking for a token that we haven't received yet?
            if (_requestedToken != null)
                if (_cache.Last != null && _requestedToken.Newer(_cache.Last.Value.RealToken))
                    return false;

            try
            {
                // if this is the first time
                if (!IsSet)
                {
                    _logger.AutoVerbose("_current is null, must be first time calling MoveNext or cache is empty");
                    _logger.AutoVerbose($"_cache is null: {_cache == null} , and has {_cache?.Count} messages");

                    // No messages?
                    if (_cache.First == null)
                    {
                        _logger.AutoVerbose("Cache is empty");
                        return false;
                    }

                    _current = _cache.First;
                    IsSet = true;
                    _logger.AutoVerbose("set _current to first message in cache");

                    // fast-forward based on requested token:
                    if (_requestedToken != null)
                    {
                        while (
                            _current.Value.RealToken.Older(_requestedToken) // has to be after this time
                            ||
                            !InStream(_current) // has to be relevent to this stream
                            )
                        {
                            if (_current.Next == null) // nothing more to fast forward
                                return false;

                            _current = _current.Next;
                        }
                        // We have found the oldest relevant message received after the time of _requestedToken
                        return true;
                    }
                    // no token given, set to the last relevant messsage
                    else
                    {
                        // Start at the end and rewind to the last relevant message:
                        _current = _cache.Last;
                        // Rewind until we find a relevant one
                        while (!InStream(_current))
                        {
                            if (_current.Previous == null) // no more?
                            {
                                // set to last message and return false
                                _current = _cache.Last;
                                return false;
                            }
                            else
                            {
                                _current = _current.Previous;
                            }
                        }
                        // We have found the last relevant one:
                        return true;
                    }
                }

                while (true)
                {
                    if (_current.Next == null) // end?
                    {
                        _logger.AutoVerbose3("no next, returning false");
                        return false;
                    }

                    _current = _current.Next;
                    _logger.AutoVerbose3("advanced to next");
                    // Find batch with the same token, no this namespace and for this stream
                    if (InStream(_current))
                    {
                        _logger.AutoVerbose("this message is for this stream, returning true");
                        return true;
                    }
                    else {
                        _logger.AutoVerbose3($"this message is NOT for this stream. This stream: {_namespace}-{_stream}, this message is for stream: {_current.Value?.StreamNamespace}-{_current.Value?.StreamGuid}");
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
            // TODO: We need to do anything here !???
        }

        private bool InStream(LinkedListNode<PipeQueueAdapterBatchContainer> node)
        {
            return node.Value?.StreamNamespace == _namespace && node.Value?.StreamGuid == _stream;
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
                    // dispose managed state (managed objects).
                }

                // free unmanaged resources (unmanaged objects) and override a finalizer below.
                // set large fields to null.

                disposedValue = true;
            }
        }

        // override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~QueueCacheRedisCursor() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}