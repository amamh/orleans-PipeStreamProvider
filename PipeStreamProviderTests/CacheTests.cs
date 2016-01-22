﻿using System;
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
        private List<object> _someEvents;
        private List<IBatchContainer> _someBatches;
        private Mock<Logger> _mockLogger;

        [TestInitialize]
        public void Init()
        {
            _streamGuid = Guid.NewGuid();
            _anotherStreamGuid = Guid.NewGuid();
            _streamNamespace = "global";
            _anotherStreamNamespace= "global2";
            _mockLogger = new Mock<Logger>();

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
            // 2 secs to purge
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
            // 2 secs to purge
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
    }
}
