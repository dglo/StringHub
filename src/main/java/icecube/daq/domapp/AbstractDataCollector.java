/* -*- mode: java; indent-tabs-mode:t; tab-width:4 -*- */

package icecube.daq.domapp;

import icecube.daq.juggler.alert.AlertQueue;
import icecube.daq.monitoring.IRunMonitor;
import icecube.daq.util.DOMInfo;
import icecube.daq.util.FlasherboardConfiguration;

import org.apache.log4j.Logger;

/**
 * Abstract base class for DataCollectors (real / sim / other).
 * This class includes the basic state drivers but lacks anything
 * that does any data collection.
 * A real DOM data collector does the following things:
 * <ul>
 * <li>Configures the DOM given a configuration class,</li>
 * <li>Collects data from the <b>hit</b>, <b>monitor</b>,
 * <b>supernova</b>, and <b>tcal</b> sources and (optionally)
 * sends the data out on channels supplied by the 'caller.'</li>
 * </ul>
 * @author krokodil
 *
 */
public abstract class AbstractDataCollector extends Thread
{
    protected int card;
    protected int pair;
    protected char dom;

    protected String mbid;
    protected String name;
    protected int major;
    protected int minor;

    protected RunLevel runLevel;
    protected DOMConfiguration config;
    protected FlasherboardConfiguration flasherConfig;
    protected boolean alwaysSoftboot = false;
    protected AlertQueue alertQueue;
    protected long firstHitTime;
    protected long lastHitTime;
    protected int runNumber = Integer.MIN_VALUE;

    private static final Logger logger = Logger.getLogger(AbstractDataCollector.class);

    public AbstractDataCollector(int card, int pair, char dom)
    {
        this.card = card;
        this.pair = pair;
        this.dom  = dom;
        mbid      = null;
        runLevel  = RunLevel.INITIALIZING;
        config    = null;
        flasherConfig = null;
        firstHitTime  = -1L;
        lastHitTime   = -1L;

        setName(card + "" + pair + dom);
    }

    public int getCard() { return card; }
    public int getPair() { return pair; }
    public char getDom() { return dom; }
	public String getMainboardId() { return mbid; }

	public void setConfig(DOMConfiguration config)
	{
	    this.config = config;
	}

	public DOMConfiguration getConfig()
	{
	    return config;
	}

	public void setFlasherConfig(FlasherboardConfiguration fbc)
	{
	    this.flasherConfig = fbc;
	}

	public FlasherboardConfiguration getFlasherConfig()
	{
	    return flasherConfig;
	}

	/**
	 * Tells the collector to begin a configure operation.  This will not happen
	 * immediately but is deferred until the collector thread's next pass through
	 * the activity loop.  When the configure is complete the state will be
	 * CONFIGURED.
	 */
	public void signalConfigure()
	{
		RunLevel tmpRunLevel = getRunLevel();

	    switch (tmpRunLevel)
	    {
	    case ZOMBIE: return;
	    case IDLE:
	    case CONFIGURED:
	        setRunLevel(RunLevel.CONFIGURING);
	        break;
	    default:
	        logger.error("Attempted to configure DOM at run level " + tmpRunLevel);
	    }
	}

	/**
	 * Asynchronously start a run.  The operation does not begin until the
	 * collector thread's next pass through the activity loop.
	 */
	public void signalStartRun()
	{
		RunLevel tmpRunLevel = getRunLevel();
		logger.info("signalStartRun");
	    switch (tmpRunLevel)
	    {
	    case CONFIGURED:
	        setRunLevel(RunLevel.STARTING);
	        break;
        default:
            logger.error("Attempted to start run on DOM at run level " + tmpRunLevel);
	    }
	}

	public void signalStopRun()
	{
		RunLevel tmpRunLevel = getRunLevel();
        switch (tmpRunLevel)
        {
        case RUNNING:
            setRunLevel(RunLevel.STOPPING);
            break;
        default:
            if (logger.isInfoEnabled()) {
                logger.info("Ignoring stop from run level " + tmpRunLevel);
            }
        }
	}

	public void signalStartSubRun()
	{
		RunLevel tmpRunLevel = getRunLevel();
	    switch (tmpRunLevel)
	    {
	    case RUNNING:
	        setRunLevel(RunLevel.STARTING_SUBRUN);
	        break;
        default:
            logger.warn("Cannot start subrun on DOM at run level " + tmpRunLevel);
	    }
	}

    public void signalPauseRun()
    {
		RunLevel tmpRunLevel = getRunLevel();
        switch (tmpRunLevel)
        {
        case RUNNING:
            setRunLevel(RunLevel.PAUSING);
            break;
        default:
            logger.warn("Ignoring pause from run level " + tmpRunLevel);
        }

    }

	public abstract void signalShutdown();

	public synchronized RunLevel getRunLevel()
	{
	    return runLevel;
	}

	public synchronized boolean isInitializing()
	{
	    return runLevel == RunLevel.INITIALIZING;
	}

	public synchronized boolean isConfiguring()
	{
	    return runLevel == RunLevel.CONFIGURING;
	}

	public synchronized boolean isConfigured()
	{
	    return runLevel == RunLevel.CONFIGURED;
	}

	public synchronized boolean isRunning()
	{
	    return runLevel == RunLevel.RUNNING;
	}

	public synchronized boolean isStopping()
	{
	    return runLevel == RunLevel.STOPPING;
	}

	public synchronized boolean isZombie()
	{
	    return runLevel == RunLevel.ZOMBIE;
	}

	/**
	 * Subclasses should override to provide last run start time in 0.1 ns ticks.
	 * @return
	 */
	public long getRunStartTime()
	{
	    return 0L;
	}

	public synchronized void setRunLevel(RunLevel runLevel)
	{
	    this.runLevel = runLevel;
	    if (logger.isDebugEnabled()) logger.debug("Run level is " + this.runLevel);
	}


	public abstract void close();

	// Monitoring facility
	public abstract long getNumHits();
	public abstract long getNumMoni();
	public abstract long getNumTcal();
	public abstract long getNumSupernova();
	public abstract long getAcquisitionLoopCount();

	public long getLastTcalTime()
	{
	    return 0L;
	}

	public double getHitRate() {
		// The naming convention is a bit obtuse here, but it is assumed this
		// means the rate in Hz of TOTAL hits. To get HLC hit rate use
		// 'getHitRateLC()'.
		return 0.0;
	}

	public double getHitRateLC() {
		// The naming convention is a bit obtuse here, but it is assumed this
		// means rate in Hz of hits with the local coincidence bits set.
		// According to john j, in an email aug 17, that means the dom is in
		// HLC mode.  To get SLC hits use 'getHitRate()'
		return 0.0;
	}

	public long getFirstHitTime() { return firstHitTime; }

	public long getLastHitTime()
	{
		RunLevel tmpRunLevel = getRunLevel();

		if (tmpRunLevel == RunLevel.CONFIGURED ||
		    tmpRunLevel == RunLevel.ZOMBIE)
		{
			return lastHitTime;
		}
		return -1L;
	}

    public long getLBMOverflowCount()
    {
        return 0L;
    }

    public void setSoftbootBehavior(boolean dcSoftboot)
    {
        alwaysSoftboot = dcSoftboot;
    }

    public void setAlertQueue(AlertQueue alertQueue)
    {
        this.alertQueue = alertQueue;
    }

    public abstract void setRunMonitor(IRunMonitor runMonitor);

    public void setDomInfo(DOMInfo domInfo)
    {
        name = domInfo.getName();
        major = domInfo.getStringMajor();
        minor = domInfo.getStringMinor();
    }

	/**
     * Set the current run number.  This is a bit off if the run is switching
	 * instead of starting, but it's the best we can do.
     *
     * @param runNumber run number
     */
	public void setRunNumber(int runNumber)
	{
		this.runNumber = runNumber;
	}
}
