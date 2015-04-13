package icecube.daq.domapp.dataprocessor;

import icecube.daq.bindery.BufferConsumer;
import icecube.daq.rapcal.RAPCal;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Implements a data stream that applies UTC timestamp reconstruction to the
 * data messages, replacing the DOM timestamp value with the equivalent UTC
 * timestamp and dispatching to a consumer.
 *
 * The following invariant is also enforced.
 *
 *    Data packets are enforced to time ordered within a configurable window.
 *
 * Note:
 * Initial implementation was taken from DataCollector.java revision 15482.
 *
 */
public class UTCDispatcher implements DataDispatcher
{

    private Logger logger = Logger.getLogger(UTCDispatcher.class);

    /*
     * Message time-order is enforced within this epsilon, in 1/10 nano.
     * Defaults to 10 microseconds.
     */
    private final static long MESSAGE_ORDERING_EPSILON =
            Integer.getInteger("icecube.daq.domapp.datacollector" +
                    ".message-ordering-epsilon",
                    10000);


    /** consumer. */
    private final BufferConsumer target;

    /** Identifies the type of data. */
    private final DataProcessor.StreamType type;

    /** Source of UTC time reconstruction. */
    protected final RAPCal rapcal;

    private final long orderingEpsilon;

    /** Holders for timestamp progression. */
    private long lastDOMClock = 0;
    private long lastUTCClock = 0;


    // if the clock jumps significantly, we will see many out-of-order
    // messages, so throttle logging.
    public final int MAX_LOGGING = 10;
    int num_logged = 0;
    // A persistent, repeating every-other out-of-order situation warrants
    // an additional throttling mechanism.
    public final int LOGGED_OCCURRENCES_PERIOD = 1000;
    private int occurrence_count = 0;


    public UTCDispatcher(final BufferConsumer target,
                         final DataProcessor.StreamType type,
                         final RAPCal rapcal)
    {
        this(target, type, rapcal, MESSAGE_ORDERING_EPSILON);
    }

    public UTCDispatcher(final BufferConsumer target,
                         final DataProcessor.StreamType type,
                         final RAPCal rapcal, final long orderingEpsilon)
    {
        this.target = target;
        this.type = type;
        this.rapcal = rapcal;
        this.orderingEpsilon = orderingEpsilon;
    }

    @Override
    public boolean hasConsumer()
    {
        return target != null;
    }


    @Override
    public void eos(final ByteBuffer eos) throws IOException
    {
        if(target != null)
        {
            target.consume(eos);
        }
    }


    @Override
    public long dispatchBuffer(final ByteBuffer buf)
            throws IOException
    {
        return dispatchBuffer(rapcal, buf);
    }


    // we are passing rapcal as an argument to support unit testing this
    // class.
    public long dispatchBuffer(final RAPCal localRapCal,
                               final ByteBuffer buf)
            throws IOException
    {
        long domclk = buf.getLong(24);
        long utc    = localRapCal.domToUTC(domclk).in_0_1ns();

        if(enforceOrdering(domclk, utc))
        {
            buf.putLong(24, utc);
            target.consume(buf);
        }
        else
        {
            //todo  After in-field discovery, Increase logging and
            //      consider dropping dom as the timestamp is off by more
            //      than epsilon.
            //
            //Note: See run 126217 for one example. eventually the
            //      dom is dropped.
        }

        return utc;
    }


    @Override
    public void dispatchHitBuffer(final int atwdChip, final ByteBuffer hitBuf,
                                  final DataStats counters)
            throws IOException
    {
        throw new Error("Unimplemented");
    }

    /**
     * Enforce an ordering policy on the message stream.
     *
     * @return true if UTC time ordering is within some epsilon.
     */
    private boolean enforceOrdering(long domclk, long utc)
    {
        //handle out-of-order hits. This could originate at the DOM, or be
        // induced by tcal updates or errors.
        if (lastUTCClock > utc)
        {
            occurrence_count++;

            long utcClockBackStep_0_1_nanos = (lastUTCClock-utc);
            boolean accept = utcClockBackStep_0_1_nanos <= orderingEpsilon;

            // detect if the ordering violation initiated at the dom or
            // is a result of applying the rapcal.
            final String reason;
            if(lastDOMClock > domclk)
            {
                reason = "Non-Contiguous " + type + " stream from DOM";
            }
            else
            {
                reason = "Non-Contiguous rapcal for DOM";
            }

            if(num_logged < MAX_LOGGING)
            {
                num_logged++;
                String action = accept ? "deliver" : "drop";
                logger.error("Out-of-order "+ type +": " +
                        "last-utc [" + lastUTCClock  + "]" +
                        " current-utc [" +utc +"]" +
                        " last-dom-clock [" + lastDOMClock + "]" +
                        " current-dom-clock [" + domclk + "]" +
                        " utc-diff [" + utcClockBackStep_0_1_nanos + "]" +
                        " (occurrence: " + occurrence_count +
                        ", reason: " + reason + ", action: "+ action + ")");

                if(num_logged == MAX_LOGGING)
                {
                    logger.error("Dampening Out-of-order logging.");
                }
            }

            return accept;
        }
        else
        {
            //Note: last clocks refer to latest-in-time messages,
            //      we will log or  lgo/drop messages until the
            //      retrograde timestamp condition is over.
            lastDOMClock = domclk;
            lastUTCClock = utc;

            // For short periods we want detailed logging, but for
            // a persistent, repeating condition we want to dampen
            // the logging
            if(occurrence_count < LOGGED_OCCURRENCES_PERIOD ||
                    occurrence_count%LOGGED_OCCURRENCES_PERIOD == 0)
            {
                num_logged=0;
            }
            return true;
        }

    }


}
