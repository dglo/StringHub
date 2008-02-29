package icecube.daq.domapp;

import cern.jet.random.Poisson;
import cern.jet.random.engine.MersenneTwister;
import cern.jet.random.engine.RandomEngine;

import icecube.daq.bindery.BufferConsumer;
import icecube.daq.bindery.MultiChannelMergeSort;
import icecube.daq.bindery.StreamBinder;
import icecube.daq.dor.DOMChannelInfo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;

import org.apache.log4j.Logger;

/**
 *
 * Simulator for StringHub DataCollector.  This simulation is fairly basic at the moment but 
 * is nevertheless useful as a generator to provide bona-fide inputs to the downstream DAQ
 * which is quite oblivious of the sham going on under its nose.
 * 
 * The SimDataCollector produces Poissonian generated hits at a rate specified in the supplied
 * configuration by the simNoiseRate parameter - typically this will come from an XML config
 * with tag of same name.
 * 
 * @author krokodil
 *
 */
public class SimDataCollector extends AbstractDataCollector
{
    private BufferConsumer  hitsConsumer;
    private BufferConsumer  moniConsumer;
    private BufferConsumer  tcalConsumer;
    private BufferConsumer  scalConsumer;
    private long            clock;
    private long            t0;
    private long            lastGenHit;           // right edge of previous hit generation time window
    private long            lastMoni;             // last moni record
    private long            lastTcal;             // last time a Tcal was generated
    private long            lastSupernova;        // last time a SN record was generated
    private long            lastBeacon;           // keep track of the beacon hits ...
    private long            numericMBID;
    private RandomEngine    rand = new MersenneTwister(new java.util.Date());
    private Poisson         poissonRandom = new Poisson(1.0, rand);
    private double          rate;
    private boolean         stopRunLoop;
    private long            numHits;
    private long            loopCounter;
    private double          pulserRate = 1.0;
    private volatile long   runStartUT = 0L;

    private Thread thread;

    private static final Logger logger = Logger.getLogger(SimDataCollector.class);

    public SimDataCollector(DOMChannelInfo chanInfo, BufferConsumer hitsConsumer)
    {
        this(chanInfo, null, hitsConsumer, null, null, null);
    }

    public SimDataCollector(DOMChannelInfo chanInfo,
                            DOMConfiguration config,
                            BufferConsumer hitsConsumer,
                            BufferConsumer moniConsumer,
                            BufferConsumer scalConsumer,
                            BufferConsumer tcalConsumer)
    {
        super(chanInfo.card, chanInfo.pair, chanInfo.dom);
        this.mbid = chanInfo.mbid;
        this.numericMBID  = Long.parseLong(this.mbid, 16);
        this.hitsConsumer = hitsConsumer;
        this.moniConsumer = moniConsumer;
        this.scalConsumer = scalConsumer;
        this.tcalConsumer = tcalConsumer;
        runLevel          = RunLevel.IDLE;
        numHits           = 0;
        loopCounter       = 0;
        if (config != null) {
            rate = config.getSimNoiseRate();
            pulserRate = config.getPulserRate();
        }
        thread = new Thread(this, "SimDataCollector-" + card + "" + pair + dom);
        thread.start();
    }

    public void close()
    {
        // do nothing
    }

    public void signalShutdown()
    {
        logger.info("Shutting down data collector [" + card + "" + pair + "" + dom + "]");
        setRunStopFlag(true);
    }

    public synchronized void setRunStopFlag(boolean val)
    {
        stopRunLoop = val;
    }

    public synchronized boolean keepRunning()
    {
        return !stopRunLoop;
    }


    public void run()
    {
        setRunStopFlag(false);

        logger.info("Entering run loop.");

        runCore();

        logger.info("Exited runCore() loop.");

        try {
            ByteBuffer otrava = MultiChannelMergeSort.eos(numericMBID);
            if (hitsConsumer != null) hitsConsumer.consume(otrava.asReadOnlyBuffer());
            if (moniConsumer != null) moniConsumer.consume(otrava.asReadOnlyBuffer());
            if (tcalConsumer != null) tcalConsumer.consume(otrava.asReadOnlyBuffer());
            if (scalConsumer != null) scalConsumer.consume(otrava.asReadOnlyBuffer());
        } catch (IOException iox) {
            iox.printStackTrace();
            logger.error(iox.getMessage());
        }
    }

    public void runCore()
    {
        Calendar now = new GregorianCalendar();
        Calendar startOfYear = new GregorianCalendar(now.get(Calendar.YEAR), 0, 1);
        t0 = startOfYear.getTimeInMillis();
        logger.debug("Start of year = " + t0);

        clock = 0L;
        logger.info("Simulated DOM at " + card + "" + pair + "" + dom + " started at dom clock " + clock);

        lastGenHit = 0L;
        lastBeacon = 0L;

        try {
            // Simulate the device open latency
            Thread.sleep(1400);

            while (keepRunning()) {
                boolean needSomeSleep = true;

                loopCounter++;

                switch (getRunLevel()) {
                case CONFIGURING:
                    // Simulate configure time
                    Thread.sleep(500);
                    setRunLevel(RunLevel.CONFIGURED);
                    logger.info("DOM is now configured.");
                    break;
                case STARTING:
                    // go to start run
                    Thread.sleep(20);
                    storeRunStartTime();
                    setRunLevel(RunLevel.RUNNING);
                    long t = System.currentTimeMillis();
                    lastGenHit = t;
                    lastBeacon = t;
                    lastMoni   = t;
                    lastTcal   = t;
                    lastSupernova = t;
                    break;
                case STARTING_SUBRUN:
                    // go to start run
                    Thread.sleep(1000L);
                    storeRunStartTime();
                    setRunLevel(RunLevel.RUNNING);
                    break;
                case RUNNING:
                    long currTime = System.currentTimeMillis();
                    int nHits = generateHits(currTime);
                    generateMoni(currTime);
                    generateTCal(currTime);
                    generateSupernova(currTime);
                    if (nHits > 0) needSomeSleep = false;
                    break;
                case STOPPING:
                    Thread.sleep(100);
                    logger.info("Stopping data collection");
                    setRunLevel(RunLevel.CONFIGURED);
                    return;
                }

                // CPU reduction action
                if (needSomeSleep) Thread.sleep(100);
            }
        } catch (InterruptedException intx) {
            intx.printStackTrace();
            logger.error(intx.getMessage());
            return;
        } catch (IOException iox) {
            iox.printStackTrace();
            logger.error(iox.getMessage());
            return;
        }
    }

    public long getRunStartTime()
    {
        return runStartUT;
    }

    private void storeRunStartTime()
    {
        runStartUT =  (System.currentTimeMillis() - t0) * 10000000L;
    }

    private int generateTCal(long currTime) throws IOException
    {
        final short[] tcalWf = new short[] {
            501, 500, 503, 505, 499, 499, 505, 500,
            501, 500, 500, 502, 500, 500, 503, 499,
            500, 499, 497, 499, 500, 500, 501, 500,
            501, 501, 500, 499, 500, 500, 501, 500,
            513, 550, 616, 690, 761, 819, 864, 898,
            925, 949, 958, 929, 856, 751, 630, 518,
            424, 346, 277, 207, 156, 137, 148,   0,
            0,   0,   0,   0,   0,   0,   0,   0
        };

        if (currTime - lastTcal < 1000L) return 0;
        lastTcal = currTime;
        final int tcalRecl = 324;
        ByteBuffer buf = ByteBuffer.allocate(tcalRecl);
        long utc = (currTime - t0) * 10000000L;
        buf.putInt(tcalRecl).putInt(202).putLong(numericMBID).putLong(0L).putLong(utc);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putShort((short) 224).putShort((short) 1);
        long dorTx = utc / 500L;
        long dorRx = dorTx + 100000L;
        long domRx = dorTx + 49000L;
        long domTx = dorTx + 51000L;
        buf.putLong(dorTx).putLong(dorRx);
        for (int i = 0; i < 64; i++) buf.putShort(tcalWf[i]);
        buf.putLong(domRx).putLong(domTx);
        for (int i = 0; i < 64; i++) buf.putShort(tcalWf[i]);
        buf.flip();
        if (tcalConsumer != null) tcalConsumer.consume(buf);
        return 1;
    }

    private int generateSupernova(long currTime) throws IOException {
        if (currTime - lastSupernova < 1000L) return 0;
        // Simulate SN wrap-around
        if (currTime - lastSupernova > 10000L) lastSupernova = currTime - 10000L;
        int dtms = (int) (currTime - lastSupernova);
        int nsn = dtms * 10000 / 16384;
        lastSupernova = currTime;
        short recl = (short) (10 + nsn);
        ByteBuffer buf = ByteBuffer.allocate(recl+32);
        long utc = (currTime - t0) * 10000000L;
        long clk = utc / 250L;
        buf.putInt(recl+32);
        buf.putInt(302);
        buf.putLong(numericMBID);
        buf.putLong(0L);
        buf.putLong(utc);
        buf.putShort(recl);
        buf.putShort((short) 300);
        buf.put((byte) ((clk >> 40) & 0xff));
        buf.put((byte) ((clk >> 32) & 0xff));
        buf.put((byte) ((clk >> 24) & 0xff));
        buf.put((byte) ((clk >> 16) & 0xff));
        buf.put((byte) ((clk >>  8) & 0xff));
        buf.put((byte) clk);
        for (int i = 0; i < nsn; i++) {
            int scaler = poissonRandom.nextInt(300 * 0.0016384);
            if (scaler > 15) scaler = 15;
            buf.put((byte) scaler);
        }
        buf.flip();
        if (scalConsumer != null) scalConsumer.consume(buf);
        return 1;
    }

    /**
     * Contains all the yucky hit generation logic
     * @return number of hits generated in the time interval
     */
    private int generateHits(long currTime) throws IOException
    {
        double dt = 1.0E-03 * (currTime - lastGenHit);
        double mu = dt * rate;
        int n = poissonRandom.nextInt(mu);
        numHits += n;
        logger.debug("Generated " + n + " events in interval " + lastGenHit + ":" + currTime);
        ArrayList<Long> eventTimes = new ArrayList<Long>(n);
        // generate n random times in the interval
        for (int i = 0; i < n; i++) {
            long rclk = 10000000L * (lastGenHit - t0) + (long) (1.0E+10 * rand.nextDouble() * dt);
            eventTimes.add(rclk);
        }

        while (true) {
            long nextBeacon = lastBeacon + (long) (1000.0 * pulserRate);
            if (nextBeacon > currTime) break;
            eventTimes.add(10000000L * (nextBeacon - t0));
            lastBeacon = nextBeacon;
        }
        // Order the event times
        Collections.sort(eventTimes);
        lastGenHit = currTime;
        for (int i = 0; i < n; i++) {
            final int recl = 112;
            ByteBuffer buf = ByteBuffer.allocate(recl);
            buf.putInt(recl);
            buf.putInt(2);
            buf.putLong(numericMBID);
            buf.putLong(0L);
            long utc = eventTimes.get(i);
            buf.putLong(utc);
            clock = utc / 500L;
            // Engineering header
            buf.putShort((short) 80).putShort((short) 2).put((byte) 0);
            // nFADC + ATWD readout config
            buf.put((byte) 0).put((byte) 3).put((byte) 0);
            // Trigger and spare field
            buf.put((byte) 2).put((byte) 0);
            // The clock word in 6 bytes
            buf.put((byte) ((clock >> 40) & 0xff));
            buf.put((byte) ((clock >> 32) & 0xff));
            buf.put((byte) ((clock >> 24) & 0xff));
            buf.put((byte) ((clock >> 16) & 0xff));
            buf.put((byte) ((clock >> 8) & 0xff));
            buf.put((byte) (clock & 0xff));
            for (int k = 0; k < 32; k++) buf.putShort((short) 138);
            buf.flip();
            logger.debug("Writing " + buf.remaining() + " byte hit at UTC = " + utc);
            hitsConsumer.consume(buf);
        }
        return n;
    }

    private int generateMoni(long currTime) throws IOException
    {
        if (currTime - lastMoni < 1000) return 0;
        // Generate an ASCII time record
        ByteBuffer moniBuf = ByteBuffer.allocate(100);
        String txt = "Hi Artur - I am a DOM monitoring record.";
        int recl = 42 + txt.length();
        moniBuf.putInt(recl);			// record length
        moniBuf.putInt(102);			// special moni code
        moniBuf.putLong(numericMBID);	// mbid as 8-byte long
        moniBuf.putLong(0L);
        long utc = (currTime - t0) * 10000000L;
        moniBuf.putLong(utc);
        clock = utc / 500L;
        moniBuf.putShort((short) (10+txt.length()));
        moniBuf.putShort((short) 0xCB);
        moniBuf.put((byte) ((clock >> 40) & 0xff));
        moniBuf.put((byte) ((clock >> 32) & 0xff));
        moniBuf.put((byte) ((clock >> 24) & 0xff));
        moniBuf.put((byte) ((clock >> 16) & 0xff));
        moniBuf.put((byte) ((clock >> 8) & 0xff));
        moniBuf.put((byte) (clock & 0xff));
        moniBuf.put(txt.getBytes());
        moniBuf.flip();
        if (moniConsumer != null) moniConsumer.consume(moniBuf);
        lastMoni = currTime;
        return 1;
    }

    public long getNumHits() {
        return numHits;
    }

    public long getNumMoni() {
        return 0;
    }

    public long getNumTcal() {
        return 0;
    }

    public long getNumSupernova() {
        return 0;
    }

    public long getAcquisitionLoopCount() {
        return loopCounter;
    }

    public String getMainboardId()
    {
        return mbid;
    }

    public void start()
    {
        // TODO Auto-generated method stub
    }
}
