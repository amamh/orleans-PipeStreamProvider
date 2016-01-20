using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Runtime;
using PipeStreamProvider.Cache;
using System.Linq;
using PipeStreamProvider;
using Orleans.Streams;
using System.Threading.Tasks;

namespace PipeStreamProviderTests
{
    [TestClass]
    public class CacheTests
    {
        private Guid _streamGuid, _anotherStreamGuid;
        private string _streamNamespace;
        private List<object> _someEvents;
        private List<IBatchContainer> _someBatches;

        [TestInitialize]
        public void Init()
        {
            _streamGuid = Guid.NewGuid();
            _anotherStreamGuid = Guid.NewGuid();
            _streamNamespace = "global";

            // Create some batches
            _someEvents = new List<object>(new object[] { 1, 2, 3, });
            var count = 10;
            var timeStamps = from s in Enumerable.Range(0, count) select (DateTime.UtcNow - TimeSpan.FromSeconds(count - s - 1));
            var tokens = from t in timeStamps select new TimeSequenceToken(t);
            var batches = from t in tokens select new PipeQueueAdapterBatchContainer(_streamGuid, _streamNamespace, _someEvents, t, null);

            _someBatches = batches.ToList<IBatchContainer>();
        }

        [TestMethod]
        public void CanAddMessagesToCache()
        {
            // 5 secs to purge
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, new MockLogger());
            cache.AddToCache(_someBatches);
            Assert.IsTrue(cache.Size == _someBatches.Count);
        }

        [TestMethod]
        public void CachePurgesOldMessages()
        {
            // 2 secs to purge
            var timeToPurge = TimeSpan.FromSeconds(2);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, new MockLogger());
            cache.AddToCache(_someBatches);
            Task.Delay(timeToPurge).Wait();

            // You have to do another add to trigger purging:
            cache.AddToCache(_someBatches.Take(1).ToList());
            // should now have only the last one added
            Assert.IsTrue(cache.Size == 1);
        }

        [TestMethod]
        public void AddMessagesThenReadShouldReadSameMessages()
        {
            // 2 secs to purge
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, new MockLogger());
            cache.AddToCache(_someBatches);

            // start time to read is the time of the first batch
            var firstBatch = _someBatches.First() as PipeQueueAdapterBatchContainer;
            var token = firstBatch.RealToken;

            var cursor = cache.GetCacheCursor(_streamGuid, _streamNamespace, token);

            foreach (var b in _someBatches)
            {
                // Move
                Assert.IsTrue(cursor.MoveNext());
                // Read the same batch
                Exception ex;
                Assert.IsTrue(b == cursor.GetCurrent(out ex));
                Assert.IsTrue(ex == null);
            }

            // Can't move any more
            Assert.IsFalse(cursor.MoveNext());
        }

        [TestMethod]
        public void AddMessagesThenReadAsDifferentStreamShouldReturnNoMessages()
        {
            // 2 secs to purge
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, new MockLogger());
            cache.AddToCache(_someBatches);

            // start time to read is the time of the first batch
            var firstBatch = _someBatches.First() as PipeQueueAdapterBatchContainer;
            var token = firstBatch.RealToken;

            var cursor = cache.GetCacheCursor(_anotherStreamGuid, _streamNamespace, token);

            // Can't move
            Assert.IsFalse(cursor.MoveNext());
        }

        [TestMethod]
        public void CannotManuallyTellCacheToPurge()
        {
            // 5 secs to purge
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, new MockLogger());
            cache.AddToCache(_someBatches);
            IList<IBatchContainer> purged;
            cache.TryPurgeFromCache(out purged); // Should we assert true/false? what should it return
            Assert.IsNull(purged);
            Assert.IsTrue(cache.Size == _someBatches.Count);
        }
    }

    // TODO: Replace with MOQ or some other mocking library
    class MockLogger : Orleans.Runtime.Logger
    {
        public override Severity SeverityLevel
        {
            get
            {
                return Severity.Off;
            }
        }

        public override void DecrementMetric(string name)
        {
            
        }

        public override void DecrementMetric(string name, double value)
        {
            
        }

        public override void Error(int logCode, string message, Exception exception = null)
        {
            
        }

        public override void IncrementMetric(string name)
        {
            
        }

        public override void IncrementMetric(string name, double value)
        {
            
        }

        public override void Info(string format, params object[] args)
        {
            
        }

        public override void Info(int logCode, string format, params object[] args)
        {
            
        }

        public override void TrackDependency(string name, string commandName, DateTimeOffset startTime, TimeSpan duration, bool success)
        {
            
        }

        public override void TrackEvent(string name, IDictionary<string, string> properties = null, IDictionary<string, double> metrics = null)
        {
            
        }

        public override void TrackException(Exception exception, IDictionary<string, string> properties = null, IDictionary<string, double> metrics = null)
        {
            
        }

        public override void TrackMetric(string name, TimeSpan value, IDictionary<string, string> properties = null)
        {
            
        }

        public override void TrackMetric(string name, double value, IDictionary<string, string> properties = null)
        {
            
        }

        public override void TrackRequest(string name, DateTimeOffset startTime, TimeSpan duration, string responseCode, bool success)
        {
            
        }

        public override void TrackTrace(string message)
        {
            
        }

        public override void TrackTrace(string message, IDictionary<string, string> properties)
        {
            
        }

        public override void TrackTrace(string message, Severity severityLevel)
        {
            
        }

        public override void TrackTrace(string message, Severity severityLevel, IDictionary<string, string> properties)
        {
            
        }

        public override void Verbose(string format, params object[] args)
        {
            
        }

        public override void Verbose(int logCode, string format, params object[] args)
        {
            
        }

        public override void Verbose2(string format, params object[] args)
        {
            
        }

        public override void Verbose2(int logCode, string format, params object[] args)
        {
            
        }

        public override void Verbose3(string format, params object[] args)
        {
            
        }

        public override void Verbose3(int logCode, string format, params object[] args)
        {
            
        }

        public override void Warn(int logCode, string message, Exception exception)
        {
            
        }

        public override void Warn(int logCode, string format, params object[] args)
        {
            
        }
    }
}
