package icecube.daq.domapp.dataprocessor;

import java.nio.ByteBuffer;

/**
 * Defines the dispatch layer.
 *
 * The dispatcher takes processed, DAQ formatted data buffers, performs
 * UTC time reconstruction of the timestamp field, and forwards the
 * data to the consumer.
 *
 * The implementations perform additional behavior such as time-order
 * enforcement., A/B buffering.
 */
public interface DataDispatcher
{

    /**
     * Supports the capability to defer dispatch, while providing
     * the caller access to the reconstructed utc time.
     */
    interface DispatchCallback
    {
        public void wasDispatched(long utc);
    }

    /**
     * Answers if a consumer is present, a potential optimization
     * for processing.
     */
    boolean hasConsumer();

    /**
     * Dispatch the EOS marker.
     *
     * @param eos The EOS marker.
     *
     * @throws DataProcessorError Error dispatching the eos.
     */
    void eos(ByteBuffer eos) throws DataProcessorError;

    /**
     * Dispatch the buffer to the consumer, performing UTC timestamp
     * reconstruction in the process
     *
     * @param buf The buffer to dispatch. The timestamp
     *            on input is a dom clock value.
     *
     * @throws DataProcessorError Error dispatching the buffer.
     */
    void dispatchBuffer(ByteBuffer buf)
            throws DataProcessorError;

    /**
     * Dispatch the buffer to the consumer, performing UTC timestamp
     * reconstruction in the process.
     *
     * @param buf The buffer to dispatch. The timestamp
     *            on input is a dom clock value.
     *            @param callback Onec available, the reconstructed UTC
     *                            timestamp value that was applied will
     *                            be presented to the callback.
     * @throws DataProcessorError Error dispatching the buffer.
     */
    void dispatchBuffer(ByteBuffer buf, DispatchCallback callback)
            throws DataProcessorError;

    /**
     * Dispatch a hit event.
     *
     * A bit of an awkward bolt-on because hit dispatching is dependent
     * on the details of the data.
     *
     * @param atwdChip Identifies the source chip.
     * @param hitBuf The hit data.
     * @param counters Parameter object for maintaining dispatch counters.
     *
     * @throws DataProcessorError Error dispatching the buffer.
     */
    void dispatchHitBuffer(final int atwdChip, final ByteBuffer hitBuf,
                           final DataStats counters) throws DataProcessorError;
}
