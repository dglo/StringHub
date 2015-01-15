/* -*- mode: java; indent-tabs-mode:t; tab-width:4 -*- */

package icecube.daq.stringhub;

import icecube.daq.domapp.AbstractDataCollector;
import icecube.daq.juggler.component.DAQConnector;

import java.util.ArrayList;

import org.apache.log4j.Logger;

/***
 * StringHub connector for data collectors.
 */
public class DOMConnector
	extends DAQConnector
{
	/** DOM data collectors. */
	private ArrayList<AbstractDataCollector> collectors;
	private static final Logger logger = Logger.getLogger(DOMConnector.class);

	/**
	 * Create a DAQ input connector.
	 *
	 * @param nch number of channels
	 */
	public DOMConnector(int nch)
	{
		super("DOMs", false);

		collectors = new ArrayList<AbstractDataCollector>();
	}

	/**
	 * Add a data collector.
	 *
	 * @param dc data collector
	 */
	public void add(AbstractDataCollector dc)
	{
		collectors.add(dc);
	}

	/**
	 * Configure data collectors.
	 */
	public void configure() throws InterruptedException
	{
		// Wait for data collectors to finish initializing
		for (AbstractDataCollector dc : collectors)
		{
			while (dc.isInitializing()) Thread.sleep(100);
			dc.signalConfigure();
		}

		// wait for things to configure
		for (AbstractDataCollector dc : collectors)
			while(dc.isConfiguring()) Thread.sleep(100);

		logger.debug("Data collector ensemble has been configured.");
	}

	/**
	 * Destroy this connector.
	 *
	 * @throws InterruptedException if there was a problem
	 */
	public void destroy()
		throws InterruptedException
	{
		stopProcessing();

		for (AbstractDataCollector dc : collectors) dc.signalShutdown();

		for (AbstractDataCollector dc : collectors)
		{
			while (dc.isAlive()) Thread.sleep(100);
			dc.close();
		}
	}

	/**
	 * Force engine to stop processing data.
	 *
	 * @throws InterruptedException if there is a problem
	 */
	public void forcedStopProcessing()
		throws InterruptedException
	{
		throw new Error("Unimplemented");
	}

	/**
	 * Return number of active channels.
	 *
	 * @return number of active channels
	 */
	public int getNumberOfChannels()
	{
		return collectors.size();
	}

	/**
	 * Get current engine state.
	 *
	 * @return state string
	 */
	public String getState()
	{
		throw new Error("Unimplemented");
	}

	/**
	 * Are <i>all</i> data collectors not running?
	 *
	 * @return <tt>true</tt> if this connector is stopped
	 */
	public boolean isStopped()
	{
		for (AbstractDataCollector dc : collectors)
			if (dc.isRunning()) return false;
		return true;
	}

	/**
	 * Are all data collectors running?
	 */
	public boolean isRunning()
	{
		return !isStopped();
	}

	/**
	 * Start background threads.
	 *
	 * @throws Exception if there is a problem
	 */
	public void start()
	{
		// do nothing
	}

	/**
	 * Start processing data.
	 *
	 * @throws InterruptedException if there is a problem
	 */
	public void startProcessing()
		throws InterruptedException
	{
		CLOOP: for (AbstractDataCollector dc : collectors)
		{
			while (!dc.isConfigured())
			{
				if (dc.isZombie()) continue CLOOP;
				Thread.sleep(100);
			}
			dc.signalStartRun();
		}
	}

	/**
	 * Stop DOM data collectors.
	 *
	 * @throws InterruptedException if there is a problem
	 */
	public void stopProcessing()
		throws InterruptedException
	{
		for (AbstractDataCollector dc : collectors)
			dc.signalStopRun();

		for (AbstractDataCollector dc : collectors)
		{
			while (dc.isStopping()) Thread.sleep(100);
		}
	}

	public ArrayList<AbstractDataCollector> getCollectors() {
		return collectors;
	}

	/**
	 * String representation.
	 *
	 * @return debugging string
	 */
	public String toString()
	{
		return "DOMConn[]";
	}
}
