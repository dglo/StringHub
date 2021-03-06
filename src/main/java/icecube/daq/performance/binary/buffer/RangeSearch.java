package icecube.daq.performance.binary.buffer;

import icecube.daq.performance.binary.buffer.RecordBuffer.MemoryMode;
import icecube.daq.performance.binary.record.RecordReader;
import sun.misc.HexDumpEncoder;

import java.io.IOException;

/**
 * Defines methods for extracting a range of data from a buffer
 * of ordered records.
 *
 */
public interface RangeSearch
{

    /**
     * Extract data from a buffer withing a range of values.
     * @param buffer The source buffer containing records ordered
     *               by the range values.
     * @param resultMode Controls whether the extracted data will be an
     *                independent copy or shared view into the source buffer.
     * @param from The inclusive starting value for the range.
     * @param to The inclusive ending value for the range.
     * @return A RecordBuffer containing the records from the source buffer
     *         which fall within the range.
     * @exception IOException An error accessing the data.
     */
    public RecordBuffer extractRange(RecordBuffer buffer,
                                     MemoryMode resultMode,
                                     long from,
                                     long to)
        throws IOException;

    /**
     * Extract data from a buffer withing a range of values utilizing
     * an index to refine the search.
     * @param buffer The source buffer containing records ordered
     *               by the range values.
     * @param resultMode Controls whether the extracted data will be an
     *                independent copy or shared view into the source buffer.
     * @param index An index of the source buffer containing (position, value)
     *              pairs used to refine the range search.
     * @param from The inclusive starting value for the range.
     * @param to The inclusive ending value for the range.
     * @return A RecordBuffer containing the records from the source buffer
     *         which fall within the range.
     * @exception IOException An error accessing the data.
     */
    public RecordBuffer extractRange(RecordBuffer buffer,
                                     MemoryMode resultMode,
                                     RecordBufferIndex index,
                                     long from, long to)
            throws IOException;


    /**
     * Basic range search utilizing a long ordering field to perform a
     * linear search of the buffer to find the range boundaries.
     */
    public class LinearSearch implements RangeSearch
    {

        private final RecordReader recordReader;
        private final RecordReader.LongField orderingField;

        public LinearSearch(final RecordReader recordReader,
                            final RecordReader.LongField orderingField)
        {
            this.recordReader = recordReader;
            this.orderingField = orderingField;
        }

        @Override
        public RecordBuffer extractRange(final RecordBuffer buffer,
                                         final MemoryMode resultMode,
                                         final long from,
                                         final long to) throws IOException
        {
            return extractRange(buffer, resultMode, from, to, 0, 0);
        }

        @Override
        public RecordBuffer extractRange(final RecordBuffer buffer,
                                         final MemoryMode resultMode,
                                         final RecordBufferIndex index,
                                         final long from,
                                         final long to) throws IOException
        {

            // use the index to drive extraction
            int startHint = index.lessThan(from);
            int endHint = index.lessThan(to);

            return extractRange(buffer, resultMode,
                    from, to, startHint, endHint);
        }

        private RecordBuffer extractRange(final RecordBuffer buffer,
                                         final MemoryMode resultMode,
                                         final long from,
                                         final long to,
                                         final int startHint,
                                         final int endHint) throws IOException
        {
            // Records are only iterable in the forward direction.  Indexes
            // can improve search performance by jumping over many records
            // rather that examining every one individually. Even so, a
            // partial linear search is required to locate the starting
            // and ending position that defines the range.
            //
            //
            // I. NO INDEX
            // Consider (from=22, to=77)
            //
            //                             |xxxxxxxxxxxxxxxxxxx|
            // -------------------------------------------------------
            // |  11 |  12  |  13 |  13  |  22  |  77  |  77  |  88  |
            // -------------------------------------------------------
            //
            // 1) We need to search forwards from the beginning of the
            //    buffer (value 11) to locate the first 22 value.
            //
            // 2) Rather than starting back at value 11, we can start the
            //    end-of-range search from the value 22 discovered in step 1
            //
            //
            //
            // II. USING START AND END INDEX
            // Consider (from=22, to=77, index=*)
            //
            //               |xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx|
            // ------------------------------------------------------------
            // |  11 |  12* |  22 |  22*  |  55*  |  77*  |  77  |  88*  |
            // -----------------------------------------------------------
            //      |                    |
            //      StartHint            EndHint
            //
            // 1) We can to search forwards from 11 to locate the
            //    first 22 value. (We can not index directly to 22 because
            //    of the possibility of the duplicate value)
            //
            // 2) We can to search forwards from 55 to locate the last value
            //    of 77. (The index does not support indexing directly to
            //    77 which would be an improvement)
            //
            //
            //
            // USING START HINT, DISCARDING END HINT
            // Consider (from=22, to=77, index=*)
            //
            //                             |xxxxxxxxxxxxxxxxxxxxx|
            // ------------------------------------------------------------
            // |  11 |  11  |  11* |  11  |  22  |  77*  |  77  |  88*  |
            // -----------------------------------------------------------
            //             |
            //             StartHint
            //             EndHint
            //
            // 1) We can to search forwards from 11 to locate the
            //    first 22 value.
            //
            // 2) Rather than starting back at value 11, we can start the
            //    end-of-range search from the value 22 discovered in step 1
            //
            //

            // find the exact range boundaries
            int first = findFirst(buffer, (startHint<0 ? 0 : startHint), from);

            if(first < 0)
            {
                return RecordBuffers.EMPTY_BUFFER;
            }

            int last = findLast(buffer, Math.max(first, endHint), to);

            // extract the data
            return resultMode.extractData(buffer, first, (last-first));
        }

        /**
         * Advance through the buffer returning the index of the first record
         * greater or equal to a value, or -1 if all records are
         * less than the value;
         */
        private int findFirst(final RecordBuffer buffer,
                              final int startingIndex, final long value)
                throws IOException
        {
            final int limit = buffer.getLength();
            int idx = startingIndex;
            long lastValue = Long.MIN_VALUE;
            while (idx < limit)
            {
                final long currentValue = orderingField.value(buffer, idx);
                if(currentValue >= value)
                {
                    return idx;
                }
                else
                {
                    if(currentValue < lastValue)
                    {
                        throwBadOrderRecordException(buffer, idx, lastValue,
                                currentValue);
                    }
                    lastValue=currentValue;
                }

                // advance, protecting against infinite loop caused
                // by malformed record data.
                final int recordLength = recordReader.getLength(buffer, idx);

                if(recordLength < 1)
                {
                    throwBadLengthException(buffer, idx);
                }
                idx += recordLength;

            }

            return -1;
        }

        /**
         * Advance through the buffer returning the index of the first record
         * greater than a value, or the length of the buffer if all records
         * are less than or equal to a value.
         *
         */
        private int findLast(RecordBuffer buffer, int startingIndex, long value)
                throws IOException
        {
            int limit = buffer.getLength();
            int idx = startingIndex;
            long lastValue = Long.MIN_VALUE;
            while (idx < limit)
            {
                long currentValue = orderingField.value(buffer, idx);
                if(currentValue > value)
                {
                    return idx;
                }
                else
                {
                    if(currentValue < lastValue)
                    {
                        throwBadOrderRecordException(buffer, idx, lastValue,
                                currentValue);
                    }
                    lastValue=currentValue;
                }
                // advance, protecting against infinite loop caused
                // by malformed record data.
                int recordLength = recordReader.getLength(buffer, idx);

                if(recordLength < 1)
                {
                    throwBadLengthException(buffer, idx);
                }
                idx += recordLength;
            }

            return idx;
        }
    }


    /**
     * Generate an IOException for a bad length field condition.
     * @param buffer The buffer containing the bad record.
     * @param index The index withing the buffer of the bad record.
     * @throws IOException An IOException capturing the details surrounding
     *                     the bad record.
     */
    public static void throwBadLengthException(RecordBuffer buffer, int index)
            throws IOException
    {
        final int length = buffer.getLength();
        final int dumpBytes = Math.min(256, (length-index) );
        final String hex = hexdump(buffer, index, dumpBytes);
        throw new IOException("Invalid record length at index: " + index +
                " of " + length +
                ", data["+index+"-" + (index+dumpBytes) + "]:\n" + hex);

    }

    /**
     * Generate an IOException for an out-of-order record condition.
     * @param buffer The buffer containing the out-of-order record.
     * @param index The index withing the buffer of the mis-ordered record.
     * @param lastValue The ordering value of the preceding record.
     * @param currentValue The ordering value of the mis-ordered record.
     * @throws IOException An IOException capturing the details surrounding
     *                     the mis-ordered record.
     */
    public static void throwBadOrderRecordException(RecordBuffer buffer,
                                                    int index, long lastValue,
                                                    long currentValue)
            throws IOException
    {
        final int length = buffer.getLength();
        final int dumpBytes = Math.min(256, (length-index) );
        final String hex = hexdump(buffer, index, dumpBytes);
        throw new IOException("Out of order record at index: " + index +
                " of " + length + ", lastUTC: " + lastValue +
                ", currentUTC: " + currentValue +
                ", data["+index+"-" + (index+dumpBytes) + "]:\n" + hex);
    }

    /**
     * Generate a hexdump string for a range of data.
     * @param buffer The data buffer.
     * @param start The start of the range.
     * @param length The length of the range.
     * @return A string containing a hexadecimal representation of the data.
     */
    public static String hexdump(RecordBuffer buffer, int start, int length)
    {
        HexDumpEncoder hex = new HexDumpEncoder();
        byte[] tmp = new byte[length];
        buffer.copyBytes(tmp, 0, start, length);
        return hex.encode(tmp);
    }


}
