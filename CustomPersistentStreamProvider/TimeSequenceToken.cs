using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PipeStreamProvider
{
    /// <summary>
    /// Works based on time and an index. A Batch will have index of -1 and all events inside it should have the same timestamp but sequenced 0...n
    /// </summary>
    [Serializable]
    public class TimeSequenceToken : StreamSequenceToken
    {
        // TODO: Do we need set access?
        /// <summary>
        /// This should always use UTC time.
        /// </summary>
        public DateTime Timestamp { get; set; }

        public int EventIndex { get; set; }

        public TimeSequenceToken(DateTime time, int eventIndex = -1)
        {
            if (time == null) throw new ArgumentNullException(nameof(time));
            Timestamp = time;
            EventIndex = eventIndex;
        }

        /// <summary>
        /// This method is helpful if this token is for a batch container. Use this method to generate a token per event
        /// </summary>
        /// <param name="eventInd"></param>
        /// <returns></returns>
        public TimeSequenceToken CreateSequenceTokenForEvent(int eventInd)
        {
            return new TimeSequenceToken(Timestamp, eventInd);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as TimeSequenceToken);
        }

        public override bool Equals(StreamSequenceToken other)
        {
            var token = other as TimeSequenceToken;
            return token != null && (token.Timestamp == Timestamp &&
                                     token.EventIndex == EventIndex);
        }

        public override int CompareTo(StreamSequenceToken other)
        {
            if (other == null)
                return 1;

            var token = other as TimeSequenceToken;
            if (token == null)
                throw new ArgumentOutOfRangeException(nameof(other));

            var difference = Timestamp.CompareTo(token.Timestamp);
            return difference != 0 ? difference : EventIndex.CompareTo(token.EventIndex);
        }

        public override int GetHashCode()
        {
            // why 397?
            return (EventIndex * 397) ^ Timestamp.GetHashCode();
        }

        public override string ToString()
        {
            // TODO: Format time
            return string.Format(CultureInfo.InvariantCulture, "[TimeSequenceToken: Timestamp={0}, EventIndex={1}]", Timestamp, EventIndex);
        }
    }
}
