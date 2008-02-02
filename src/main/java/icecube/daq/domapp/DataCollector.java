/* -*- mode: java; indent-tabs-mode:f; tab-width:4 -*- */

package icecube.daq.domapp;

import icecube.daq.bindery.StreamBinder;
import icecube.daq.dor.DOMChannelInfo;
import icecube.daq.dor.Driver;
import icecube.daq.dor.GPSException;
import icecube.daq.dor.GPSInfo;
import icecube.daq.dor.IDriver;
import icecube.daq.dor.TimeCalib;
import icecube.daq.rapcal.AbstractRAPCal;
import icecube.daq.rapcal.LeadingEdgeRAPCal;
import icecube.daq.rapcal.RAPCal;
import icecube.daq.rapcal.RAPCalException;
import icecube.daq.rapcal.ZeroCrossingRAPCal;
import icecube.daq.util.UTC;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.ClosedByInterruptException;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

/**
 * A data collection engine which drives the readout of the hits,
 * monitor, tcal, and supernova streams from a single DOM channel.
 * The object is a multi-threaded state machine.  The caller 
 * commands certain state changes which trigger a response and
 * a state switch by the object's execution thread.  This allows
 * for non-blocking state switching on the caller's side.
 *   
 * The output streams are passed into the object at construction
 * time and can be anything that supports the WritableByteChannel
 * interface.  The streaming data is output in 'TestDAQ' format 
 * for all outputs with the structure: 
 * <table>
 * <tr>
 * <th>Offset</th>
 * <th>Size</th>
 * <th>Data</th>
 * </tr>
 * <tr>
 * <td> 0 </td>
 * <td> 4 </td>
 * <td>Record length</td>
 * </tr>
 * <tr>
 * <td> 4 </td>
 * <td> 4 </td>
 * <td>Record ID</td>
 * </tr>
 * <tr>
 * <td> 8 </td>
 * <td> 8 </td>
 * <td>Mainboard ID</td>
 * </tr>
 * <tr>
 * <td> 16 </td>
 * <td> 8 </td>
 * <td>Reserved - must be 0</td>
 * </tr>
 * <tr>
 * <td> 24 </td>
 * <td> 8 </td>
 * <td>UT timestamp</td>
 * </tr>
 * </table> Supported records types are
 * <dl>
 * <dt>2</dt>
 * <dd>DOM engineering hit record</dd>
 * <dt>3</dt>
 * <dd>DOM delta-compressed hit records (including SLC hits)</dd>
 * <dt>102</dt>
 * <dd>DOM monitoring records</dd>
 * <dt>202</dt>
 * <dd>TCAL records</dd>
 * <dt>302</dt>
 * <dd>Supernova scaler records</dd>
 * </dl>
 * 
 * @author krokodil
 * 
 */
public class DataCollector extends AbstractDataCollector
{
    private int                 card;
    private int                 pair;
    private char                dom;
    private long                numericMBID;
    private IDOMApp             app;
    private GPSInfo             gps;
    private UTC                 gpsOffset;
    private RAPCal              rapcal;
    private IDriver             driver;
    private boolean             stop_thread;
    private WritableByteChannel hitsSink;
    private WritableByteChannel moniSink;
    private WritableByteChannel tcalSink;
    private WritableByteChannel supernovaSink;
    
    private static final Logger         logger  = Logger.getLogger(DataCollector.class);
    private static final DecimalFormat  fmt     = new DecimalFormat("#0.000000000");
    
    // TODO - replace these with properties-supplied constants
    // for now they are totally reasonable
    private long                threadSleepInterval   = 50;
    private long                lastDataRead          = 0;
    private long                dataReadInterval      = 10;
    private long                lastMoniRead          = 0;
    private long                moniReadInterval      = 1000;
    private long                lastTcalRead          = 0;
    private long                tcalReadInterval      = 1000;
    private long                lastSupernovaRead     = 0;
    private long                supernovaReadInterval = 1000;
    
    private int                 rapcalExceptionCount  = 0;
    private int                 validRAPCalCount;

    private int                 numHits               = 0;
    private int                 numMoni               = 0;
    private int                 numSupernova          = 0;
    private int                 loopCounter           = 0;
    private long                lastDataUT;
    private long                lastMoniUT;
    private long                lastTcalUT;
    private long                lastSupernovaUT;
    private volatile long       runStartUT = 0L;

    private ByteBuffer          daqHeader;
    private long                nextSupernovaDomClock;
    private HitBufferAB         abBuffer;
    private int                 numSupernovaGaps;
    
    /**
     * A helper class to deal with the now-less-than-trivial
     * hit buffering which circumvents the hit out-of-order
     * issues.
     * @author kael
     *
     */
    private class HitBufferAB
    {
        class Element implements Comparable<Element>
        {
            int recl;
            int fmtid;
            long domClock;
            ByteBuffer buf;
            
            Element(int recl, int fmtid, long domClock, ByteBuffer buf)
            {
                this.recl = recl;
                this.fmtid = fmtid;
                this.domClock = domClock;
                this.buf = buf;
            }

            public int compareTo(Element o)
            {
                if (this.domClock > o.domClock)
                    return 1;
                else if (this.domClock < o.domClock)
                    return -1;
                else
                    return 0;
            }
        }
        
        private LinkedList<Element> alist, blist; 
        /** 
         * 'Head' elements for A/B to improve performance by
         * relegating the costly LinkedList accesses to those
         * rare cases when you need them.
         */
        private Element ahead, bhead;
        
        HitBufferAB()
        {
            alist = new LinkedList<Element>();
            blist = new LinkedList<Element>();
            ahead = null;
            bhead = null;
        }
        
        void pushA(int recl, int fmtid, long domClock, ByteBuffer buf)
        {
            Element e = new Element(recl, fmtid, domClock, buf);
            if (logger.isDebugEnabled())
                logger.debug("Pushed element into A buffer: domClock = " + domClock + " # A = " 
                        + alist.size() + " # B = " + blist.size());
            // If head element occupied then push to end of list - else put onto head
            if (ahead != null) 
                alist.addLast(e);
            else
                ahead = e;
        }
        
        void pushB(int recl, int fmtid, long domClock, ByteBuffer buf)
        {
            Element e = new Element(recl, fmtid, domClock, buf);
            if (logger.isDebugEnabled())
                logger.debug("Pushed element into B buffer: domClock = " + domClock + " # A = " 
                        + alist.size() + " # B = " + blist.size());
            // If head element occupied then push to end of list - else put onto head
            if (bhead != null) 
                blist.addLast(e);
            else
                bhead = e;
        }
        
        private Element popA()
        {
           Element x = ahead;
           ahead = null;
           if (!alist.isEmpty()) ahead = alist.removeFirst();
           return x;
        }
        
        private Element popB()
        {
            Element x = bhead;
            bhead = null;
            if (!blist.isEmpty()) bhead = blist.removeFirst();
            return x;
        }
        
        Element pop()
        {
            if (ahead == null || bhead == null) return null;
            if (ahead.compareTo(bhead) < 0)
                return popA();
            else
                return popB();
        }
    }
    
    
    public DataCollector(DOMChannelInfo chInfo, WritableByteChannel out) throws IOException, MessageException
    {
        this(chInfo.card, chInfo.pair, chInfo.dom, null, out, null, null, null);
    }

    public DataCollector(int card, int pair, char dom, WritableByteChannel out) throws IOException,
            MessageException
    {
        this(card, pair, dom, null, out, null, null, null);
    }

    public DataCollector(
            int card, int pair, char dom, 
            DOMConfiguration config,
            WritableByteChannel outHits,
            WritableByteChannel outMoni, WritableByteChannel outSupernova, WritableByteChannel outTcal)
            throws IOException, MessageException
    {
        this(card, pair, dom, config, 
                outHits, outMoni, outSupernova, outTcal, Driver.getInstance(),
                new ZeroCrossingRAPCal(), null);
    }

    public DataCollector(int card, int pair, char dom, 
            DOMConfiguration config,
            WritableByteChannel outHits,
            WritableByteChannel outMoni, 
            WritableByteChannel outSupernova, 
            WritableByteChannel outTcal,
            IDriver driver, RAPCal rapcal, IDOMApp app) throws IOException, MessageException
    {
        super(card, pair, dom);
        this.card = card;
        this.pair = pair;
        this.dom = dom;
        this.hitsSink = outHits;
        // this.moniSink = null;
        // this.tcalSink = null;
        // this.supernovaSink = null;
        this.moniSink = outMoni;
        this.tcalSink = outTcal;
        this.supernovaSink = outSupernova;
        this.driver = driver;
        this.rapcal = rapcal;
        this.app = app;
        this.config = config;
        
        logger.debug("DC " + canonicalName() + " hitsSink = " + hitsSink);
        logger.debug("DC " + canonicalName() + " moniSink = " + moniSink);
        logger.debug("DC " + canonicalName() + " tcalSink = " + tcalSink);
        logger.debug("DC " + canonicalName() + " supernovaSink = " + supernovaSink);

        gps = null;

        runLevel = RunLevel.IDLE;
        gpsOffset = new UTC(0L);
        daqHeader = ByteBuffer.allocate(32);
        abBuffer  = new HitBufferAB();
        start();
    }

    public void close()
    {
        if (app != null) app.close();
        try
        {
            if (hitsSink != null)
            {
                hitsSink.close();
                hitsSink = null;
            }
            if (moniSink != null)
            {
                moniSink.close();
                moniSink = null;
            }
            if (tcalSink != null)
            {
                tcalSink.close();
                tcalSink = null;
            }
            if (supernovaSink != null)
            {
                supernovaSink.close();
                supernovaSink = null;
            }
        }
        catch (IOException iox)
        {
            iox.printStackTrace();
            logger.error("Error closing pipe sinks: " + iox.getMessage());
        }
    }

    private String canonicalName()
    {
        return "[" + card + "" + pair + dom + "]";
    }

    /**
     * Applies the configuration in this.config to the DOM
     * 
     * @throws MessageException
     */
    private void configure(DOMConfiguration config) throws MessageException
    {
        logger.info("Configuring DOM on " + canonicalName());
        long configT0 = System.currentTimeMillis();

        app.setMoniIntervals(
                config.getHardwareMonitorInterval(), 
                config.getConfigMonitorInterval(),
                config.getFastMonitorInterval()
                );

        if (config.isDeltaCompressionEnabled())
            app.setDeltaCompressionFormat();
        else
            app.setEngineeringFormat(config.getEngineeringFormat());

        if (config.getHV() >= 0)
        {
            app.enableHV();
            app.setHV(config.getHV());
        }
        else
        {
            app.disableHV();
        }

        // DAC setting
        for (byte dac_ch = 0; dac_ch < 16; dac_ch++)
            app.writeDAC(dac_ch, config.getDAC(dac_ch));
        app.setMux(config.getMux());
        app.setTriggerMode(config.getTriggerMode());
        
        // If trigger mode is Flasher then don't touch the pulser mode
        if (config.getTriggerMode() != TriggerMode.FB)
        {
            if (config.getPulserMode() == PulserMode.BEACON)
                app.pulserOff();
            else
                app.pulserOn();
        }
        
        app.setPulserRate(config.getPulserRate());
        LocalCoincidenceConfiguration lc = config.getLC();
        app.setLCType(lc.getType());
        app.setLCMode(lc.getRxMode());
        app.setLCTx(lc.getTxMode());
        app.setLCSource(lc.getSource());
        app.setLCSpan(lc.getSpan());
        app.setLCWindow(lc.getPreTrigger(), lc.getPostTrigger());
        app.setCableLengths(lc.getCableLengthUp(), lc.getCableLengthDn());
        app.enableSupernova(config.getSupernovaDeadtime(), config.isSupernovaSpe());
        app.setScalerDeadtime(config.getScalerDeadtime());

        // Do the pedestal subtraction
        if (config.getPedestalSubtraction())
        {
            // WARN - this is done /w/ HV applied - probably need to
            // screen in DOMApp for spurious pulses
            app.collectPedestals(200, 200, 200);
        }

        long configT1 = System.currentTimeMillis();
        logger.info("Finished DOM configuration - " + canonicalName() + "; configuration took "
                + (configT1 - configT0) + " milliseconds.");
    }

    private long genericDataDispatch(int recl, int fmtid, long domClock, ByteBuffer in, WritableByteChannel out)
            throws IOException
    {
        daqHeader.clear();
        daqHeader.putInt(recl + 32).putInt(fmtid).putLong(numericMBID).putInt(0).putInt(0);
        long utc = rapcal.domToUTC(domClock).in_0_1ns();
        daqHeader.putLong(utc);
        daqHeader.flip();
        GatheringByteChannel g = (GatheringByteChannel) out;
        ByteBuffer[] bufferArray = new ByteBuffer[] { daqHeader, in };
        while (in.remaining() > 0)
        {
            long nw = g.write(bufferArray);
            if (logger.isDebugEnabled())
                logger.debug(
                        "In DC " + canonicalName() + " - type = " + 
                        fmtid + " wrote " + nw + " bytes to " + out
                        );
        }
        return utc;
    }

    private void dataProcess(ByteBuffer in) throws IOException
    {
        // TODO - I created a number of less-than-elegant hacks to
        // keep performance at acceptable level such as minimal
        // decoding of hit records. This should be cleaned up.

        int buffer_limit = in.limit();

        // create records from aggregrate message data returned from DOMApp
        while (in.remaining() > 0)
        {
            int pos = in.position();
            short len = in.getShort();
            short fmt = in.getShort();
            if (hitsSink != null)
            {
                
                int atwdChip;
                long domClock;
                ByteBuffer dbuf;
                switch (fmt)
                {
                case 1:
                case 2: // Engineering format data
                    // strip out the clock word - advance pointer
                    in.position(pos + 4);
                    atwdChip = in.get() & 1;
                    in.position(pos + 10);
                    domClock = DOMAppUtil.decodeSixByteClock(in);
                    in.position(pos);
                    in.limit(in.position() + len);
                    numHits++;
                    dbuf = ByteBuffer.allocate(in.remaining());
                    dbuf.put(in);
                    dbuf.flip();
                    if (atwdChip == 0)
                        abBuffer.pushA(len, 2, domClock, dbuf);
                    else
                        abBuffer.pushB(len, 2, domClock, dbuf);
                    while (true) 
                    {
                        HitBufferAB.Element e = abBuffer.pop();
                        if (e == null) break;
                        lastDataUT = genericDataDispatch(e.recl, e.fmtid, e.domClock, e.buf, hitsSink);
                    }
                    in.limit(buffer_limit);
                    break;
                case 144: // Delta compressed data
                    int clkMSB = in.getShort();
                    in.getShort();
                    // Note byte order change for delta message buffers
                    in.order(ByteOrder.LITTLE_ENDIAN);
                    while (in.remaining() > 0)
                    {
                        // create an additional byte buffer to handle re-format of
                        // the delta payload - I want to insert the re-assembled
                        // DOMClock in front of the compression header
                        // Advance past compression hit header
                        int word1 = in.getInt();
                        int word2 = in.getInt();
                        int word3 = in.getInt();
                        int hitSize = word1 & 0x7ff;
                        atwdChip = (word1 >> 11) & 1;
                        domClock = (((long) clkMSB) << 32) | (((long) word2) & 0xffffffffL);
                        short version = 1;
                        short pedestal = 0;
                        if (config.getPedestalSubtraction()) pedestal = 1;
                        try
                        {
                            in.limit(in.position() + hitSize - 12);
                            dbuf = ByteBuffer.allocate(hitSize + 10);
                            dbuf.order(ByteOrder.LITTLE_ENDIAN);
                            dbuf.putShort((short) 1);
                            dbuf.putShort(version);
                            dbuf.putShort(pedestal);
                            dbuf.putLong(domClock);
                            dbuf.putInt(word1);
                            dbuf.putInt(word3);
                            dbuf.put(in);
                            numHits++;
                            dbuf.flip();
                            if (atwdChip == 0)
                                abBuffer.pushA(dbuf.remaining(), 3, domClock, dbuf);
                            else
                                abBuffer.pushB(dbuf.remaining(), 3, domClock, dbuf);
                            while (true) 
                            {
                                HitBufferAB.Element e = abBuffer.pop();
                                if (e == null) break;
                                lastDataUT = genericDataDispatch(e.recl, e.fmtid, e.domClock, e.buf, hitsSink);
                            }
                            in.limit(buffer_limit);
                        }
                        catch (IllegalArgumentException illargx)
                        {
                            logger.error("Caught IllegalArgument Exception in dataProcess: dumping compressed header words: " 
                                    + Integer.toHexString(word1) 
                                    + ", " + Integer.toHexString(word2)
                                    + ", " + Integer.toHexString(word3)
                                    + " - in.position() = " + in.position()
                                    + " - in.remaining() = " + in.remaining()
                                    + " - in.capacity() = " + in.capacity());
                            throw illargx;
                        }
                    }
                    in.order(ByteOrder.BIG_ENDIAN);
                    break;
                }
            }
            else
            {
                // skip over this unknown record
                logger.warn("skipping over unknown record type " + fmt + " of " + len + " bytes.");
                in.position(in.position() + len - 4);
            }
        }
    }

    private void moniProcess(ByteBuffer in) throws IOException
    {
        while (in.remaining() > 0)
        {
            // logger.debug("processing monitoring record - " + in.remaining() +
            // " bytes remaining.");
            MonitorRecord monitor = MonitorRecordFactory.createFromBuffer(in);
            if (monitor instanceof AsciiMonitorRecord &&
                    logger.isDebugEnabled()) logger.debug(monitor.toString());
            if (moniSink != null)
            {
                numMoni++;
                lastMoniUT = genericDataDispatch(
                        monitor.getLength(),
                        102, 
                        monitor.getClock(), 
                        monitor.getBuffer(),
                        moniSink
                        );
            }
        }
    }

    /**
     * Process the TCAL data. Please be aware of and excuse the awful coding
     * here. The TCAL reference time passed to the dispatcher is the DOM
     * waveform receive time. TODO The time transforms are hideous and need to
     * be scrubbed.
     * 
     * @param tcal
     * @param gps
     * @throws IOException
     */
    private void tcalProcess(TimeCalib tcal, GPSInfo gps) throws IOException
    {
        if (tcalSink != null)
        {
            ByteBuffer buffer = ByteBuffer.allocate(500);
            tcal.writeUncompressedRecord(buffer);
            buffer.put(gps.getBuffer());
            buffer.flip();
            lastTcalUT = tcal.getDorTx().in_0_1ns();
            genericDataDispatch(buffer.remaining(), 202, tcal.getDomRx().in_0_1ns() / 250L, buffer, tcalSink);
        }
    }

    private void supernovaProcess(ByteBuffer in) throws IOException
    {
        while (in.remaining() > 0)
        {
            SupernovaPacket spkt = SupernovaPacket.createFromBuffer(in);
            // Check for gaps in SN data
            if ((nextSupernovaDomClock != 0L) && (spkt.getClock() != nextSupernovaDomClock) && numSupernovaGaps++ < 100)
                logger.warn("Gap or overlap in SN rec: next = " + nextSupernovaDomClock 
                        + " - current = " + spkt.getClock());
            
            nextSupernovaDomClock = spkt.getClock() + (spkt.getScalers().length << 16);
            
            if (supernovaSink != null)
            {
                numSupernova++;
                lastSupernovaUT = genericDataDispatch(spkt.getLength(), 302, spkt.getClock(), spkt.getBuffer(),
                        supernovaSink);
            }
        }
    }

    public synchronized void signalShutdown()
    {
        stop_thread = true;
    }

    private void storeRunStartTime() throws InterruptedException
    {
        try
        {
            TimeCalib rst = driver.readTCAL(card, pair, dom);
            runStartUT = rapcal.domToUTC(rst.getDomTx().in_0_1ns() / 250L).in_0_1ns();
        }
        catch (IOException iox)
        {
            logger.warn("I/O error on TCAL read to determine run start time.");
        }
    }
    
    public String toString()
    {
        return getName();
    }

    private void execRapCal()
    {
        try
        {
            gps = driver.readGPS(card);
            gpsOffset = gps.getOffset();
            TimeCalib tcal = driver.readTCAL(card, pair, dom);
            rapcal.update(tcal, gpsOffset);

            validRAPCalCount++;

            if (getRunLevel().equals(RunLevel.RUNNING))
            {
                tcalProcess(tcal, gps);
            }

        }
        catch (RAPCalException rcex)
        {
            rapcalExceptionCount++;
            rcex.printStackTrace();
            logger.warn("Got RAPCal exception");
        }
        catch (GPSException gpsx)
        {
            gpsx.printStackTrace();
            logger.warn("Got GPS exception");
        }
        catch (IOException iox)
        {
            iox.printStackTrace();
            logger.warn(iox);
        }
        catch (InterruptedException intx)
        {
            intx.printStackTrace();
            logger.warn("Got interrupted exception");
        }
    }

    /**
     * The process is controlled by the runLevel state flag ...
     * <dl>
     * <dt>CONFIGURING (1)</dt>
     * <dd>signal a configure needed - successful configure will propagate the
     * state to CONFIGURED.</dd>
     * <dt>CONFIGURED (2)</dt>
     * <dd>the DOM is now configured and ready to start.</dd>
     * <dt>STARTING (3)</dt>
     * <dd>the DOM has received the start signal and is in process of starting
     * run.</dd>
     * <dt>RUNNING (4)</dt>
     * <dd>the thread is acquiring data.</dd>
     * <dt>STOPPING (5)</dt>
     * <dd>the DOM has received the stop signal and is in process of returning
     * to the CONFIGURED state.</dd>
     * </dl>
     */
    public void run()
    {

        lastDataUT = 0L;
        lastMoniUT = 0L;
        lastTcalUT = 0L;
        lastSupernovaUT = 0L;
        nextSupernovaDomClock = 0L;
        numSupernovaGaps = 0;
        
        logger.info("Begin data collection thread");
        
        // Create a watcher timer
        Timer watcher = new Timer(getName() + "-timer");

        try
        {
            runcore(watcher);
        }
        catch (Exception x)
        {
            x.printStackTrace();
            logger.error("Intercepted error in DataCollector runcore: " + x);
            // HACK set run level to ZOMBIE so that controller knows
            // that this channel has expired and does not wait.
            setRunLevel(RunLevel.ZOMBIE);
        }

        watcher.cancel();
        
        // clear interrupted flag if it is set
        interrupted();

        // Make sure eos is written
        try
        {
            if (hitsSink != null) hitsSink.write(StreamBinder.endOfStream());
            if (moniSink != null) moniSink.write(StreamBinder.endOfMoniStream());
            if (tcalSink != null) tcalSink.write(StreamBinder.endOfTcalStream());
            if (supernovaSink != null) supernovaSink.write(StreamBinder.endOfSupernovaStream());
            logger.info("Wrote EOS to streams.");
        }
        catch (IOException iox)
        {
            logger.error(iox);
        }
        
        logger.info("End data collection thread.");

    } /* END OF run() METHOD */

    /** Wrap up softboot -> domapp behavior */
    private IDOMApp softbootToDomapp() throws IOException, InterruptedException
    {
        driver.commReset(card, pair, dom);
        driver.softboot (card, pair, dom);

        FileNotFoundException savedEx = null;

        for (int i = 0; i < 2; i++) {
            driver.commReset(card, pair, dom);
        
            if (app == null)
            {
                // If app is null it implies the collector has deferred
                // opening of the DOR devfile to the thread.
                try 
                {
                    app = new DOMApp(this.card, this.pair, this.dom);
                    break;
                } 
                catch (FileNotFoundException ex) 
                {
					logger.error("Trial "+i+": Open of "+card+""+pair+dom+" failed!");
					logger.error("Driver comstat for "+card+""+pair+dom+":\n"+driver.getComstat(card,pair,dom));
					logger.error("FPGA regs for card "+card+":\n"+driver.getFPGARegs(card));
                    app = null;
                    savedEx = ex;
                }
            }
        }
                
        if (app == null) 
        {
            if (savedEx != null) throw savedEx;
            throw new FileNotFoundException("Couldn't open DOMApp");
        } 
        else if (savedEx != null) 
        {
            logger.error("Successful DOMApp retry after initial failure", savedEx);
        }

        app.transitionToDOMApp();
        return app;
    }

    /**
     * This is a deeper run - basically I want a nice way of efficiently getting
     * the stop signals written - a simple return to a wrapper which handles
     * this seems best. So the thread run method will handle that recovery
     * process
     */
    private void runcore(Timer watcher) throws Exception
    {
        InterruptorTask intTask = new InterruptorTask(this);
        watcher.schedule(intTask, 30000L, 20000L);

		driver.resetComstat(card, pair, dom);

        // Wrap up in retry loop - sometimes getMainboardID fails/times out
        // DOM is in a strange state here
        // this is a workaround for "Type 3" dropped DOMs
        numericMBID = 0;
        int NT      = 2;
        for(int i = 0; i < NT; i++) 
        {
            intTask.ping();
            try 
            {
                app = softbootToDomapp();
                try 
                {
                    mbid = app.getMainboardID();
                } 
                catch (MessageException ex) 
                {
                    // if exception is wrapping a ClosedByInterruptException,
                    //   then throw the original exception
                    if (ex.getCause() != null &&
                        ex.getCause() instanceof ClosedByInterruptException)
                    {
                        throw (ClosedByInterruptException) ex.getCause();
                    }

                    // otherwise, throw the MessageException
                    throw ex;
                }
                numericMBID = Long.valueOf(mbid, 16).longValue();
                break;
            } 
            catch (ClosedByInterruptException ex) 
            {
                // clear the interrupt so it doesn't cause future problems
                Thread.interrupted();

                // log exception and continue
                logger.error("Timeout on trial "+i+" getting DOM ID", ex);
				logger.error("Driver comstat for "+card+""+pair+dom+":\n"+driver.getComstat(card,pair,dom));
				logger.error("FPGA regs for card "+card+":\n"+driver.getFPGARegs(card));
				app = null; /* We have to do this to guarantee that we reopen when we retry */
            }
        }
        
        if (numericMBID == 0)
            throw new Exception("Couldn't get DOM MB ID after "+NT+" trials.");
        
		intTask.ping();
        logger.info("Found DOM " + mbid + " running " + app.getRelease());

        // Grab 2 RAPCal data points to get started
        for (int nTry = 0; nTry < 10 && validRAPCalCount < 2; nTry++) execRapCal();
        lastTcalRead = System.currentTimeMillis();

        /*
         * Workhorse - the run loop
         */
        logger.info("Entering run loop");

        while (!stop_thread && !interrupted())
        {
            long t = System.currentTimeMillis();
            boolean tired = true;

            // Ping the watchdog task
            intTask.ping();

            loopCounter++;

            /* Do TCAL and GPS -- this always runs regardless of the run state */
            if (t - lastTcalRead >= tcalReadInterval)
            {
                logger.debug("Doing TCAL - runLevel is " + getRunLevel());
                lastTcalRead = t;
                execRapCal();
            }

            switch (getRunLevel())
            {
            case RUNNING:
                // Time to do a data collection?
                if (t - lastDataRead >= dataReadInterval)
                {
                    lastDataRead = t;
                    try 
                    { 
                        // Get debug information during Alpaca failures
                        ByteBuffer data = app.getData();
                        if (data.remaining() > 0) tired = false;
                        dataProcess(data);
                    } 
                    catch (IllegalArgumentException ex) 
                    {
                        logger.error("Caught & re-raising IllegalArgumentException");
                        logger.error("Driver comstat for "+card+""+pair+dom+":\n"+driver.getComstat(card,pair,dom));
                        logger.error("FPGA regs for card "+card+":\n"+driver.getFPGARegs(card));
                        throw ex;
                    }
                }
                
                // What about monitoring?
                if (t - lastMoniRead >= moniReadInterval)
                {
                    lastMoniRead = t;
                    ByteBuffer moni = app.getMoni();
                    if (moni.remaining() > 0)
                    {
                        moniProcess(moni);
                        tired = false;
                    }
                }
                if (t - lastSupernovaRead > supernovaReadInterval)
                {
                    lastSupernovaRead = t;
                    while (true)
                    {
                        ByteBuffer sndata = app.getSupernova();
                        if (sndata.remaining() > 0)
                        {
                            supernovaProcess(sndata);
                            tired = false;
                            break;
                        }
                    }
                }
                break;
                
            case CONFIGURING:
                /* Need to handle a configure */
                logger.info("Got CONFIGURE signal.");
                configure(config);
                logger.info("DOM is configured.");
                setRunLevel(RunLevel.CONFIGURED);
                break;
                
            case STARTING:
                logger.info("Got START RUN signal " + canonicalName());
                app.beginRun();
                storeRunStartTime();
                logger.info("DOM is running.");
                setRunLevel(RunLevel.RUNNING);
                break;
                
            case STARTING_SUBRUN:
                setRunLevel(RunLevel.STOPPING_SUBRUN);
                app.endRun();
                setRunLevel(RunLevel.CONFIGURING);
                if (flasherConfig != null)
                {
                    logger.info("Starting flasher subrun");
                    DOMConfiguration tempConfig = new DOMConfiguration(config);
                    tempConfig.setHV(-1);
                    tempConfig.setTriggerMode(TriggerMode.FB);
                    tempConfig.setLC(new LocalCoincidenceConfiguration());
                    tempConfig.setEngineeringFormat(
                            new EngineeringRecordFormat((short) 0, new short[] { 0, 0, 0, 128 })
                            );
                    tempConfig.setMux(MuxState.FB_CURRENT);
                    configure(tempConfig);
                    app.beginFlasherRun(
                            (short) flasherConfig.getBrightness(), 
                            (short) flasherConfig.getWidth(),
                            (short) flasherConfig.getDelay(), 
                            (short) flasherConfig.getMask(), 
                            (short) flasherConfig.getRate()
                            );
                }
                else
                {
                    logger.info("Returning to non-flashing state");
                    configure(config);
                    app.beginRun();
                }
                storeRunStartTime();
                setRunLevel(RunLevel.RUNNING);
                break;
                
            case PAUSING:
                logger.info("Got PAUSE RUN signal " + canonicalName());
                app.endRun();
                setRunLevel(RunLevel.CONFIGURED);
                break;
                
            case STOPPING:
                logger.info("Got STOP RUN signal " + canonicalName());
                app.endRun();
                // Write the end-of-stream token
                if (hitsSink != null) hitsSink.write(StreamBinder.endOfStream());
                if (moniSink != null) moniSink.write(StreamBinder.endOfMoniStream());
                if (tcalSink != null) tcalSink.write(StreamBinder.endOfTcalStream());
                if (supernovaSink != null) supernovaSink.write(StreamBinder.endOfSupernovaStream());
                setRunLevel(RunLevel.CONFIGURED);
                break;
                
            }

            if (tired)
            {
                logger.debug("Runcore loop is tired - sleeping " + threadSleepInterval + " ms.");
                try
                {
                    Thread.sleep(threadSleepInterval);
                }
                catch (InterruptedException intx)
                {
                    logger.warn("Interrupted.");
                }
            }
        } /* END RUN LOOP */
    } /* END METHOD */

    public long getRunStartTime()
    {
        return runStartUT;
    }
    
    public long getNumHits()
    {
        return numHits;
    }

    public long getNumMoni()
    {
        return numMoni;
    }

    public long getNumTcal()
    {
        return validRAPCalCount;
    }

    public long getNumSupernova()
    {
        return numSupernova;
    }

    public long getAcquisitionLoopCount()
    {
        return loopCounter;
    }

    /**
     * A watchdog timer task to make sure data stream does not die.
     */
    class InterruptorTask extends TimerTask
    {
        Thread  thread;
        boolean pinged;
        
        InterruptorTask(Thread thread)
        {
            this.thread = thread;
            this.pinged = false;
        }

        public void run()
        {
            if (!pinged) thread.interrupt();
            pinged = false;
        }

        synchronized void ping()
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("pinged at " + fmt.format(System.nanoTime() * 1.0E-09));
            }
            pinged = true;
        }
    }

    @Override
    public long getLastTcalTime()
    {
        // TODO Auto-generated method stub
        return lastTcalUT;
    }
    
}
