package icecube.daq.domapp;

import icecube.daq.bindery.StreamBinder;
import icecube.daq.dor.DOMChannelInfo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.log4j.Logger;

import cern.jet.random.Poisson;
import cern.jet.random.engine.MersenneTwister;
import cern.jet.random.engine.RandomEngine;

/**
 * 
 * @author krokodil
 *
 */
public class SimDataCollector extends AbstractDataCollector {

	private WritableByteChannel hitsOut;
	private WritableByteChannel moniOut;
	private WritableByteChannel tcalOut;
	private WritableByteChannel supernovaOut;
	private String mbid;
	private int card;
	private int pair;
	private char dom;
	private long clock;
	private long t0;
	private long lastGenHit;           // right edge of previous hit generation time window
	private long lastMoni;             // last moni record
	private long lastTcal;             // last time a Tcal was generated
	private long lastSupernova;        // last time a SN record was generated
    private long lastBeacon;           // keep track of the beacon hits ...
	private long numericMBID;
	private RandomEngine rand = new MersenneTwister(new java.util.Date());
	private Poisson poissonRandom = new Poisson(1.0, rand);
	private int runLevel;
	private double rate;
	private boolean stopRunLoop;
	private long numHits;
	private long loopCounter;
    private double pulserRate = 1.0;

	private final static Logger logger = Logger.getLogger(SimDataCollector.class);
	
	public SimDataCollector(DOMChannelInfo chanInfo, WritableByteChannel hitsOut)
	{
		this(chanInfo, hitsOut, null, null, null);
	}

	public SimDataCollector(DOMChannelInfo chanInfo, 
			WritableByteChannel hitsOut,
			WritableByteChannel moniOut,
			WritableByteChannel tcalOut,
			WritableByteChannel supernovaOut)
	{
		this.mbid = chanInfo.mbid;
		this.card = chanInfo.card;
		this.pair = chanInfo.pair;
		this.dom  = chanInfo.dom;
		this.numericMBID = Long.parseLong(this.mbid, 16);
		this.hitsOut = hitsOut;
		this.moniOut = moniOut;
		//this.tcalOut = tcalOut;
		this.supernovaOut = supernovaOut;
		this.moniOut = null;
		this.tcalOut = null;
		this.supernovaOut = null;
		runLevel  = IDLE;
		numHits = 0;
		loopCounter = 0;
	}

	public void close() 
    { 
        try {
            
            if (hitsOut != null) {
                hitsOut.close();
                hitsOut = null;
            }
            if (moniOut != null) {
                moniOut.close();
                moniOut = null;
            }
            if (tcalOut != null) {
                tcalOut.close();
                tcalOut = null;
            }
            if (supernovaOut != null) {
                supernovaOut.close();
                supernovaOut = null;
            }
        } catch (IOException iox) {
            iox.printStackTrace();
            logger.error("Error closing pipe sinks: " + iox.getMessage());
        }        
    }

	public void setConfig(DOMConfiguration config) 
	{
	    this.rate = config.getSimNoiseRate();
        logger.info("Setting simDOM noise rate to " + rate);
        this.pulserRate = config.getPulserRate();
        logger.info("Setting simDOM pulser rate to " + pulserRate);
	}

	public void signalConfigure() 
	{
		logger.info("Got (DOM) configure message - telling thread to configure DOMs.");
		if (queryDaqRunLevel() > CONFIGURED)
		{
			logger.error("Cannot configure DOM (even a simulated one) in state " + runLevel);
			throw new IllegalStateException();
		}
		logger.info("Configuring simulated DataCollector " + getName());
		setRunLevel(CONFIGURING);
	}

	public void signalStartRun() 
	{
		logger.info("Got start run message - telling collection thread to start.");
		if (queryDaqRunLevel() != CONFIGURED)
		{
			logger.error("Cannot start run on DOM in state " + runLevel);
			throw new IllegalStateException();
		}
		setRunLevel(STARTING);
	}

	public void signalStopRun() 
	{
		logger.info("Got stop run message - telling collection thread to stop.");
		if (queryDaqRunLevel() != RUNNING)
		{
			logger.error("Cannot stop run that is not running -- runLevel = " + runLevel);
			throw new IllegalStateException();
		}
		setRunLevel(STOPPING);
	}

	public synchronized int queryDaqRunLevel() 
	{
		return runLevel;
	}

	private synchronized void setRunLevel(int runLevel)
	{
		this.runLevel = runLevel;
	}
	
	@Override
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
	
	@Override 
	public void run()
	{
		setRunStopFlag(false);
		
		runCore();
        
        logger.info("Exited runCore() loop.");
        
		try
		{
			if (hitsOut != null) hitsOut.write(StreamBinder.endOfStream());
			if (moniOut != null) moniOut.write(StreamBinder.endOfStream());
			if (tcalOut != null) tcalOut.write(StreamBinder.endOfStream());
			if (supernovaOut != null) supernovaOut.write(StreamBinder.endOfStream());
		}
		catch (IOException iox)
		{
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
        
		try
		{
			// Simulate the device open latency
			Thread.sleep(1400);

			while (keepRunning())
			{
				boolean needSomeSleep = true;

				loopCounter++;

				if (queryDaqRunLevel() == CONFIGURING) 
				{
					// Simulate configure time
					Thread.sleep(500);
					setRunLevel(CONFIGURED);
                    logger.info("DOM is now configured.");
				}
				else if (queryDaqRunLevel() == STARTING)
				{
					// go to start run
					Thread.sleep(20);
					setRunLevel(RUNNING);
					long t = System.currentTimeMillis();
					lastGenHit = t;
                    lastBeacon = t;
					lastMoni   = t;
					lastTcal   = t;
					lastSupernova = t;
				}
				else if (queryDaqRunLevel() == RUNNING)
				{
					long currTime = System.currentTimeMillis();
					int nHits = generateHits(currTime);
					generateMoni(currTime);
                    generateSupernova(currTime);
					if (nHits > 0) needSomeSleep = false; 
				}
				else if (queryDaqRunLevel() == STOPPING)
				{
					Thread.sleep(100);
					logger.info("Stopping data collection");
					if (hitsOut != null) hitsOut.write(StreamBinder.endOfStream());
					if (moniOut != null) moniOut.write(StreamBinder.endOfStream());
					if (tcalOut != null) tcalOut.write(StreamBinder.endOfStream());
					if (supernovaOut != null) supernovaOut.write(StreamBinder.endOfStream());
					logger.debug("Flushed binders.");
					setRunLevel(CONFIGURED);
				}
				
				// CPU reduction action
				if (needSomeSleep) Thread.sleep(100);
			}
		}
		catch (InterruptedException intx)
		{
			intx.printStackTrace();
			logger.error(intx.getMessage());
			return;
		}
		catch (IOException iox)
		{
			iox.printStackTrace();
			logger.error(iox.getMessage());
			return;
		}
		
	}
	
	private int generateSupernova(long currTime) throws IOException {
        if (currTime - lastSupernova < 1000L) return 0;
        // Simulate SN wrap-around
        if (currTime - lastSupernova > 10000L) lastSupernova = currTime - 10000L;
        int dtms = (int) (currTime - lastSupernova);
        int nsn = dtms * 10000 / 16384;
        lastSupernova = currTime;
        short recl = (short) (10 + nsn);
        long clk = (currTime - t0) * 10000000L;
	    ByteBuffer buf = ByteBuffer.allocate(recl);
        buf.putShort(recl);
        buf.putShort((short) 300);
        buf.put((byte) ((clk >> 40) & 0xff));
        buf.put((byte) ((clk >> 32) & 0xff));
        buf.put((byte) ((clk >> 24) & 0xff));
        buf.put((byte) ((clk >> 16) & 0xff));
        buf.put((byte) ((clk >>  8) & 0xff));
        buf.put((byte) clk);
        for (int i = 0; i < nsn; i++)
        {
            int scaler = poissonRandom.nextInt(rate * 0.0016384);
            if (scaler > 15) scaler = 15;
            buf.put((byte) scaler);
        }
        buf.flip();
        if (supernovaOut != null) supernovaOut.write(buf); 
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
		for (int i = 0; i < n; i++)
		{
			long rclk = 10000000L * (lastGenHit - t0) + (long) (1.0E+10 * rand.nextDouble() * dt);
			eventTimes.add(rclk);
		}
   
        while (true)
        {
            long nextBeacon = lastBeacon + (long) (1000.0 * pulserRate);
            if (nextBeacon > currTime) break;
            eventTimes.add(10000000L * (nextBeacon - t0));
            lastBeacon = nextBeacon;
        }
		// Order the event times
		Collections.sort(eventTimes);
		lastGenHit = currTime;
		for (int i = 0; i < n; i++)
		{
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
			hitsOut.write(buf);
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
		if (moniOut != null) moniOut.write(moniBuf);
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

}
