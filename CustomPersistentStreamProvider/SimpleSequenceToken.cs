using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PipeStreamProvider
{
    // Same as EventSequenceToken
    [Serializable]
    public class SimpleSequenceToken : StreamSequenceToken
    {
        public long SequenceNumber { get; set; }

        public int EventIndex { get; set; }

        public SimpleSequenceToken(long seqNumber)
        {
            SequenceNumber = seqNumber;
            EventIndex = 0;
        }

        internal SimpleSequenceToken(long seqNumber, int eventInd)
        {
            SequenceNumber = seqNumber;
            EventIndex = eventInd;
        }

        public SimpleSequenceToken NextSequenceNumber()
        {
            return new SimpleSequenceToken(SequenceNumber + 1, EventIndex);
        }

        public SimpleSequenceToken CreateSequenceTokenForEvent(int eventInd)
        {
            return new SimpleSequenceToken(SequenceNumber, eventInd);
        }

        internal static long Distance(SimpleSequenceToken first, SimpleSequenceToken second)
        {
            return first.SequenceNumber - second.SequenceNumber;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as SimpleSequenceToken);
        }

        public override bool Equals(StreamSequenceToken other)
        {
            var token = other as SimpleSequenceToken;
            return token != null && (token.SequenceNumber == SequenceNumber &&
                                     token.EventIndex == EventIndex);
        }

        public override int CompareTo(StreamSequenceToken other)
        {
            if (other == null)
                return 1;

            var token = other as SimpleSequenceToken;
            if (token == null)
                throw new ArgumentOutOfRangeException(nameof(other));

            var difference = SequenceNumber.CompareTo(token.SequenceNumber);
            return difference != 0 ? difference : EventIndex.CompareTo(token.EventIndex);
        }

        public override int GetHashCode()
        {
            // why 397?
            return (EventIndex * 397) ^ SequenceNumber.GetHashCode();
        }

        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "[SimpleSequenceToken: SeqNum={0}, EventIndex={1}]", SequenceNumber, EventIndex);
        }
    }
}
