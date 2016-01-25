using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Runtime;
using PipeStreamProvider.Cache;
using System.Linq;
using PipeStreamProvider;
using Orleans.Streams;
using System.Threading.Tasks;
using Moq;

namespace PipeStreamProviderTests
{
    [TestClass]
    public class CacheTests
    {
        private Guid _streamGuid, _anotherStreamGuid;
        private string _streamNamespace, _anotherStreamNamespace;
        private List<List<object>> _someEvents;
        private List<IBatchContainer> _someBatches, _otherBatches;
        private Mock<Logger> _mockLogger;

        [TestInitialize]
        public void Init()
        {
            _streamGuid = Guid.NewGuid();
            _anotherStreamGuid = Guid.NewGuid();
            _streamNamespace = "global";
            _anotherStreamNamespace = "global2";
            _mockLogger = new Mock<Logger>();

            // Create some batches
            var count = 10;
            // { {0}, {1}, {2}, ... {count -1} }
            _someEvents = (from i in Enumerable.Range(0, count) select Enumerable.Range(i,1).Cast<object>().ToList()).ToList();
            
            // { DateTime.Now -count seconds, ...., DateTime.Now }
            var timeStamps = from s in Enumerable.Range(0, count) select (DateTime.UtcNow - TimeSpan.FromSeconds(count - s - 1));
            var tokens = (from t in timeStamps select new TimeSequenceToken(t)).ToList();

            var batches = from i in Enumerable.Range(0, count) select new PipeQueueAdapterBatchContainer(_streamGuid, _streamNamespace, _someEvents[i], tokens[i], null);
            var otherBatches = from i in Enumerable.Range(0, count) select new PipeQueueAdapterBatchContainer(_anotherStreamGuid, _streamNamespace, _someEvents[i], tokens[i], null);

            _someBatches = batches.ToList<IBatchContainer>();
            _otherBatches = otherBatches.ToList<IBatchContainer>();
        }

        [TestMethod]
        public void CanAddMessagesToCache()
        {
            // 5 secs to purge
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);
            cache.AddToCache(_someBatches);
            Assert.IsTrue(cache.Size == _someBatches.Count);
        }

        [TestMethod]
        public void CachePurgesOldMessages()
        {
            // 2 secs to purge
            var timeToPurge = TimeSpan.FromSeconds(2);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);
            cache.AddToCache(_someBatches);
            Task.Delay(timeToPurge).Wait();

            // You have to do another add to trigger purging:
            cache.AddToCache(_someBatches.Take(1).ToList());
            // should now have only the last one added
            Assert.IsTrue(cache.Size == 1);
        }

        [TestMethod]
        public void ReadAsDifferentStreamShouldReturnNoMessages()
        {
            // 2 secs to purge
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);
            cache.AddToCache(_someBatches);

            // start time to read is the time of the first batch
            var firstBatch = _someBatches.First() as PipeQueueAdapterBatchContainer;
            var token = firstBatch.RealToken;

            var cursor = cache.GetCacheCursor(_anotherStreamGuid, _streamNamespace, token);

            // Can't move
            Assert.IsFalse(cursor.MoveNext());
        }

        [TestMethod]
        public void ReadAsDifferentStreamNamespaceShouldReturnNoMessages()
        {
            // 2 secs to purge
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);
            cache.AddToCache(_someBatches);

            // start time to read is the time of the first batch
            var firstBatch = _someBatches.First() as PipeQueueAdapterBatchContainer;
            var token = firstBatch.RealToken;

            var cursor = cache.GetCacheCursor(_streamGuid, _anotherStreamNamespace, token);

            // Can't move
            Assert.IsFalse(cursor.MoveNext());
        }

        [TestMethod]
        public void ReadAsDifferentStreamWithNoTokenShouldReturnNoMessages2()
        {
            // 2 secs to purge
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);
            cache.AddToCache(_someBatches);

            var cursor = cache.GetCacheCursor(_anotherStreamGuid, _streamNamespace, null);

            // Can't move
            Assert.IsFalse(cursor.MoveNext());
        }

        [TestMethod]
        public void ReadAsDifferentStreamNamespaceWithNoTokenShouldReturnNoMessages()
        {
            // 2 secs to purge
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);
            cache.AddToCache(_someBatches);

            var cursor = cache.GetCacheCursor(_streamGuid, _anotherStreamNamespace, null);

            // Can't move
            Assert.IsFalse(cursor.MoveNext());
        }

        [TestMethod]
        public void CannotManuallyTellCacheToPurge()
        {
            // 5 secs to purge
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);
            cache.AddToCache(_someBatches);
            IList<IBatchContainer> purged;
            cache.TryPurgeFromCache(out purged); // Should we assert true/false? what should it return
            Assert.IsNull(purged);
            Assert.IsTrue(cache.Size == _someBatches.Count);
        }

        [TestMethod]
        public void ReplayFromLastMessageWhenNoTokenGiven()
        {
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);
            cache.AddToCache(_someBatches);

            // Give a null token
            var cursor = cache.GetCacheCursor(_streamGuid, _streamNamespace, null);
            Exception ex;
            // Should be able to get the last message
            Assert.IsTrue(cursor.MoveNext());
            Assert.IsTrue(cursor.GetCurrent(out ex) == _someBatches.Last());
            Assert.IsNull(ex);
            // That should have been the last message, so there should be no more to read after it
            Assert.IsFalse(cursor.MoveNext());
            // Still on the same message:
            Assert.IsTrue(cursor.GetCurrent(out ex) == _someBatches.Last());
            Assert.IsNull(ex);
        }

        [TestMethod]
        public void ReplayFromStart()
        {
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);
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
        public void ReplayFromSpecificPoint()
        {
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);
            cache.AddToCache(_someBatches);

            // start time to read is the time of the first batch
            var batchesShouldBeReplayed = _someBatches.Skip(3);
            // Get a token the same as the first in these batches
            var token = (batchesShouldBeReplayed.First() as PipeQueueAdapterBatchContainer).RealToken;

            var cursor = cache.GetCacheCursor(_streamGuid, _streamNamespace, token);

            foreach (var b in batchesShouldBeReplayed)
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
        public void NoMessagesWhenTokenRequestedIsTooNew()
        {
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);
            cache.AddToCache(_someBatches);

            // get last token:
            var lastToken = (_someBatches.Last() as PipeQueueAdapterBatchContainer).RealToken;
            var newerToken = new TimeSequenceToken(lastToken.Timestamp.AddMilliseconds(1));

            var cursor = cache.GetCacheCursor(_streamGuid, _streamNamespace, newerToken);
            Assert.IsFalse(cursor.MoveNext());
        }

        [TestMethod]
        public void EmptyCacheSouldntMoveAndShouldReturnNull()
        {
            var timeToPurge = TimeSpan.FromSeconds(5);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);

            Exception ex;
            var cursor = cache.GetCacheCursor(_streamGuid, _streamNamespace, null);
            Assert.IsFalse(cursor.MoveNext());
            Assert.IsNull(cursor.GetCurrent(out ex));
            Assert.IsNull(ex);


            cursor = cache.GetCacheCursor(_streamGuid, _streamNamespace, new TimeSequenceToken(DateTime.Today));
            Assert.IsFalse(cursor.MoveNext());
            Assert.IsNull(cursor.GetCurrent(out ex));
            Assert.IsNull(ex);
        }

        [TestMethod]
        public void ReplayOnlyRelevantMessages()
        {
            var timeToPurge = TimeSpan.FromSeconds(10);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);

            // add batches for this stream:
            cache.AddToCache(_someBatches.Take(5).ToList());
            // add batches for another stream:
            cache.AddToCache(_otherBatches.Take(5).ToList());
            // add batches for this stream:
            cache.AddToCache(_someBatches.Skip(5).ToList());
            // add batches for another stream:
            cache.AddToCache(_otherBatches.Skip(5).ToList());

            var firstBatch = _someBatches.First() as PipeQueueAdapterBatchContainer;
            var token = firstBatch.RealToken;
            // We only want to read messages for stream with this GUID in this namespace:
            var cursor = cache.GetCacheCursor(_streamGuid, _streamNamespace, new TimeSequenceToken(DateTime.Today));

            // check that we read only the relevant batches for this stream
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

        /// <summary>
        /// When a client cannot read fast enough and the last message it was on gets purged, it should be pushed forward
        /// </summary>
        [TestMethod]
        public void LaggingCursorShouldBeForwardedWhenMessagesArePurged()
        {
            var timeToPurge = TimeSpan.FromSeconds(7);
            var cache = new QueueCache(QueueId.GetQueueId(0), timeToPurge, _mockLogger.Object);
            cache.AddToCache(_someBatches.Take(5).ToList());

            // get a cursor on the first message:
            var cursor = cache.GetCacheCursor(_streamGuid, _streamNamespace, new TimeSequenceToken(DateTime.Today));

            // Set it on first message:
            cursor.MoveNext();
            var moreBatches = _someBatches.Skip(5).ToList();
            // trigger purge by adding again, the previous batches are 10, 9, ..., 6 seconds old so the earliest 3 will be purged given 7 seconds to purge
            cache.AddToCache(moreBatches);
            Assert.IsTrue(cache.Size == timeToPurge.Seconds);
            // cursor now should be forwarded and should read batches at indices: 3, 4, .. count-1
            Exception ex;
            foreach (var b in _someBatches.Skip(3))
            {
                Assert.IsTrue(cursor.MoveNext());
                var current = cursor.GetCurrent(out ex);
                Assert.IsTrue(b == current);
                Assert.IsNull(ex);
            }
            // no more
            Assert.IsFalse(cursor.MoveNext());
        }
    }
}
