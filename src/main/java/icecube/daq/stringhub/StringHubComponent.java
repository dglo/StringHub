/* -*- mode: java; indent-tabs-mode:t; tab-width:4 -*- */
package icecube.daq.stringhub;

import icecube.daq.bindery.SecondaryStreamConsumer;
import icecube.daq.bindery.StreamBinder;
import icecube.daq.common.DAQCmdInterface;
import icecube.daq.configuration.XMLConfig;
import icecube.daq.domapp.AbstractDataCollector;
import icecube.daq.domapp.DOMConfiguration;
import icecube.daq.domapp.DataCollector;
import icecube.daq.domapp.SimDataCollector;
import icecube.daq.dor.DOMChannelInfo;
import icecube.daq.dor.Driver;
import icecube.daq.io.PayloadDestinationOutputEngine;
import icecube.daq.juggler.component.DAQCompException;
import icecube.daq.juggler.component.DAQComponent;
import icecube.daq.juggler.component.DAQConnector;
import icecube.daq.juggler.mbean.MemoryStatistics;
import icecube.daq.juggler.mbean.SystemStatistics;
import icecube.daq.monitoring.MonitoringData;
import icecube.daq.monitoring.DataCollectorMonitor;
import icecube.daq.payload.*;
import icecube.daq.sender.RequestReader;
import icecube.daq.sender.Sender;
import icecube.daq.util.DOMRegistry;
import icecube.daq.util.DeployedDOM;
import icecube.daq.trigger.control.StringTriggerHandler;
import icecube.daq.trigger.control.IStringTriggerHandler;
import icecube.daq.trigger.control.ITriggerControl;
import icecube.daq.trigger.component.GlobalConfiguration;
import icecube.daq.trigger.config.TriggerBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.dom4j.Node;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.xml.sax.SAXException;

class BindSource
{
	private static final Logger logger = Logger.getLogger(BindSource.class);

	private String name;
	private SelectableChannel hitSource;
	private SelectableChannel moniSource;
	private SelectableChannel tcalSource;
	private SelectableChannel snSource;

	private StreamBinder hitBind;
	private StreamBinder moniBind;
	private StreamBinder tcalBind;
	private StreamBinder snBind;

	BindSource(String name, SelectableChannel hitSource,
			   SelectableChannel moniSource, SelectableChannel tcalSource,
			   SelectableChannel snSource)
	{
		this.name = name;
		this.hitSource = hitSource;
		this.moniSource = moniSource;
		this.tcalSource = tcalSource;
		this.snSource = snSource;
	}

	String getName()
	{
		return name;
	}

	void closeChannels()
	{
		try {
			if (hitSource != null) hitSource.close();
			if (moniSource != null) moniSource.close();
			if (tcalSource != null) tcalSource.close();
			if (snSource != null) snSource.close();
		} catch (IOException iox) {
			iox.printStackTrace();
			logger.error("Error closing pipe sources: " + iox.getMessage());
		}
	}

	SelectableChannel getHitsSource()
	{
		return hitSource;
	}

	SelectableChannel getMoniSource()
	{
		return moniSource;
	}

	SelectableChannel getTcalSource()
	{
		return tcalSource;
	}

	SelectableChannel getSupernovaSource()
	{
		return snSource;
	}
	
}

public class StringHubComponent extends DAQComponent
{

	private static final Logger logger = Logger.getLogger(StringHubComponent.class);
	
    private int hubId;
	private boolean isSim = false;
	private Driver driver = Driver.getInstance();
	private Sender sender;
	private ByteBufferCache bufferManager;
	private MasterPayloadFactory payloadFactory;
	private DOMRegistry domRegistry;
	private PayloadDestinationOutputEngine   moniPayloadDest, tcalPayloadDest, supernovaPayloadDest; 
	private DOMConnector conn = null;
	private List<DOMChannelInfo> activeDOMs;
	private List<BindSource> bindSources = null;
	private StreamBinder hitsBind, moniBind, tcalBind, snBind;
	private String configurationPath;
	private String configured = "NO";
	private int nch;
	private DataCollectorMonitor collectorMonitor;

    private boolean enableTriggering = false;
    private ISourceID sourceId;
    private IStringTriggerHandler triggerHandler;

    public StringHubComponent(int hubId) throws Exception
	{
		super(DAQCmdInterface.DAQ_STRING_HUB, hubId);
	
        this.hubId = hubId;
        final String COMPONENT_NAME = DAQCmdInterface.DAQ_STRING_HUB;

		bufferManager  = new ByteBufferCache(64, 250000000L, 200000000L, "PyrateBufferManager");
		addCache(bufferManager);
		addMBean(bufferManager.getCacheName(), bufferManager);

		addMBean("jvm", new MemoryStatistics());
		addMBean("system", new SystemStatistics());

		payloadFactory = new MasterPayloadFactory(bufferManager);
		sender         = new Sender(hubId, payloadFactory);
		isSim          = (hubId >= 1000 && hubId < 2000);
		nch            = 0;
		
		logger.info("starting up StringHub component " + hubId);

        // Setup the output engine
        PayloadDestinationOutputEngine hitOut = new PayloadDestinationOutputEngine(COMPONENT_NAME, hubId, "hitOut");
        hitOut.registerBufferManager(bufferManager);

        // Rule is component xx80 - xx99 -> icetop
        if ((hubId % 100) > 80)
            addEngine(DAQConnector.TYPE_ICETOP_HIT, hitOut);
        else
            addEngine(DAQConnector.TYPE_STRING_HIT, hitOut);

        // Check if triggering is enabled
        IPayloadDestinationCollection hitColl;
        if (enableTriggering) {
            sourceId = SourceIdRegistry.getISourceIDFromNameAndId(COMPONENT_NAME, hubId);
            triggerHandler = new StringTriggerHandler(sourceId);
            triggerHandler.setMasterPayloadFactory(payloadFactory);
            triggerHandler.setPayloadDestinationCollection(hitOut.getPayloadDestinationCollection());

            // This is the output of the Sender
            IPayloadDestination payloadDestination = new ByteBufferPayloadDestination(triggerHandler, bufferManager);
            hitColl = new PayloadDestinationCollection(payloadDestination);
        } else {

            // This is the output of the Sender
            hitColl = hitOut.getPayloadDestinationCollection();
        }

        sender.setHitOutputDestination(hitColl);
        
        RequestReader reqIn;
        try {
            reqIn = new RequestReader(COMPONENT_NAME, sender, payloadFactory);
        } catch (IOException ioe) {
            throw new Error("Couldn't create RequestReader", ioe);
        }
        addMonitoredEngine(DAQConnector.TYPE_READOUT_REQUEST, reqIn);

        PayloadDestinationOutputEngine dataOut =
            new PayloadDestinationOutputEngine(COMPONENT_NAME, hubId, "dataOut");
        dataOut.registerBufferManager(bufferManager);
        addMonitoredEngine(DAQConnector.TYPE_READOUT_DATA, dataOut);

        IPayloadDestinationCollection dataColl = dataOut.getPayloadDestinationCollection();
        sender.setDataOutputDestination(dataColl);

        MonitoringData monData = new MonitoringData();
        monData.setSenderMonitor(sender);
        addMBean("sender", monData);
		
		collectorMonitor = new DataCollectorMonitor();
		addMBean("datacollectormonitor", collectorMonitor);
		
        // Following are the payload output engines for the secondary streams
        moniPayloadDest = new PayloadDestinationOutputEngine(COMPONENT_NAME, hubId, "moniOut");
        moniPayloadDest.registerBufferManager(bufferManager);
        addMonitoredEngine(DAQConnector.TYPE_MONI_DATA, moniPayloadDest);
        tcalPayloadDest = new PayloadDestinationOutputEngine(COMPONENT_NAME, hubId, "tcalOut");
        tcalPayloadDest.registerBufferManager(bufferManager);
        addMonitoredEngine(DAQConnector.TYPE_TCAL_DATA, tcalPayloadDest);
        supernovaPayloadDest = new PayloadDestinationOutputEngine(COMPONENT_NAME, hubId, "supernovaOut");
        supernovaPayloadDest.registerBufferManager(bufferManager);
        addMonitoredEngine(DAQConnector.TYPE_SN_DATA, supernovaPayloadDest);
    }

	@Override
	public void setGlobalConfigurationDir(String dirName) 
	{
		super.setGlobalConfigurationDir(dirName);
		configurationPath = dirName;
		logger.info("Setting the ueber configuration directory to " + configurationPath);
        // get a reference to the DOM registry - useful later
        try {
			domRegistry = DOMRegistry.loadRegistry(configurationPath);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		} catch (SAXException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	/**
	 * This method will force the string hub to query the driver for a list of DOMs.
	 * For a DOM to be detected its cardX/pairY/domZ/id procfile must report a valid
	 * non-zero DOM mainboard ID.
	 * @throws IOException
	 */
	public void discover() throws IOException, DocumentException
	{
		if (isSim)
		{
			ArrayList<DeployedDOM> attachedDOMs = domRegistry.getDomsOnString(getNumber());
			activeDOMs = new ArrayList<DOMChannelInfo>(attachedDOMs.size());
			for (DeployedDOM dom : attachedDOMs)
			{
				int card = (dom.getStringMinor()-1) / 8;
				int pair = ((dom.getStringMinor()-1) % 8) / 2;
				char aorb = 'A';
				if (dom.getStringMinor() % 2 == 1) aorb = 'B';
				activeDOMs.add(new DOMChannelInfo(dom.getMainboardId(), card, pair, aorb));
			}
		}
		else
		{
			activeDOMs = driver.discoverActiveDOMs();
			logger.info("Found " + activeDOMs.size() + " active DOMs.");
		}
	}
	
	/**
	 * StringHub responds to a configure request from the controller
	 */
	public void configuring(String configName) throws DAQCompException 
	{

		String realism;
		
		if (isSim)
			realism = "SIMULATION";
		else
			realism = "REAL DOMS";

		configured = "YES";

		if (bindSources != null) 
			for (BindSource b : bindSources)
				b.closeChannels();

		bindSources = new ArrayList<BindSource>();

		try 
		{
			// Lookup the connected DOMs
			discover();
			
			// Parse out tags from 'master configuration' file
			File domConfigsDirectory = new File(configurationPath, "domconfigs");
			File masterConfigFile = new File(configurationPath, configName + ".xml");
			FileInputStream fis = new FileInputStream(masterConfigFile);
			
			SAXReader r = new SAXReader();
			Document doc = r.read(fis);

			XMLConfig xmlConfig = new XMLConfig();
			List<Node> configNodeList = doc.selectNodes("runConfig/domConfigList");
			logger.info("Number of domConfigNodes found: " + configNodeList.size());
			for (Node configNode : configNodeList) {
				String tag = configNode.getText();
				File configFile = new File(domConfigsDirectory, tag + ".xml");
				logger.info("Configuring " + realism 
							+ " - loading config from " 
							+ configFile.getAbsolutePath());			
				xmlConfig.parseXMLConfig(new FileInputStream(configFile));
			}

			fis.close();

			// Find intersection of discovered / configured channels
			nch = 0;
			for (DOMChannelInfo chanInfo : activeDOMs)
				if (xmlConfig.getDOMConfig(chanInfo.mbid) != null) nch++;
	
			logger.info("Configuration successfully loaded - Intersection(DISC, CONFIG).size() = " + nch);
			
			// Must make sure to release file resources associated with the previous
			// runs since we are throwing away the collectors and starting from scratch
			if (conn != null) conn.destroy();

			conn = new DOMConnector(nch);
				
			for (DOMChannelInfo chanInfo : activeDOMs)
			{
				DOMConfiguration config = xmlConfig.getDOMConfig(chanInfo.mbid);
				if (config == null) continue;

				String cwd = chanInfo.card + "" + chanInfo.pair + chanInfo.dom;

				Pipe hitPipe = Pipe.open();
				Pipe moniPipe = Pipe.open();
				Pipe tcalPipe = Pipe.open();
				Pipe snPipe = Pipe.open();

				BindSource bs =
					new BindSource(cwd, hitPipe.source(),
								   moniPipe.source(), tcalPipe.source(),
								   snPipe.source());
				bindSources.add(bs);

				AbstractDataCollector dc;
				if (isSim)
				{
					dc = new SimDataCollector(chanInfo, hitPipe.sink(), 
											  moniPipe.sink(), snPipe.sink(), 
											  tcalPipe.sink());
				}
				else
				{
					dc = new DataCollector(
							chanInfo.card, chanInfo.pair, chanInfo.dom, 
							hitPipe.sink(), moniPipe.sink(), snPipe.sink(), tcalPipe.sink()
						);
				}

				/* queue up the config */
				dc.setConfig(config);
				conn.add(dc);
				logger.debug("Starting new DataCollector thread on (" + cwd + ").");
				dc.start();
				logger.debug("DataCollector thread on (" + cwd + ") started.");				
			}

			logger.info("Starting up HKN1 sorting trees...");
			
			// Still need to get the data collectors to pick up and do something with the config
			conn.configure();

			collectorMonitor.setConnector(conn);
		}
		
		catch (FileNotFoundException fnx)
		{
			logger.error("Could not find the configuration file.");
			throw new DAQCompException(fnx.getMessage());
		}
		catch (IOException iox)
		{
			iox.printStackTrace();
			logger.error("Caught IOException - " + iox.getMessage());
			throw new DAQCompException(iox.getMessage());
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			throw new DAQCompException(e.getMessage());
		}


        // If triggers are enabled, configure them
        if (enableTriggering) {
            configureTrigger(configName);
        }

    }

	/**
	 * Controller wants stringhub to start sending data.  Tell DOMs to start up.
	 */
	public void starting() throws DAQCompException
	{
		logger.info("Have I been configured? " + configured);

		SecondaryStreamConsumer monitorConsumer   = new SecondaryStreamConsumer(hubId, bufferManager, moniPayloadDest);
		SecondaryStreamConsumer supernovaConsumer =	new SecondaryStreamConsumer(hubId, bufferManager, supernovaPayloadDest);
		SecondaryStreamConsumer tcalConsumer      = new SecondaryStreamConsumer(hubId,bufferManager, tcalPayloadDest);

        if (Boolean.getBoolean("icecube.daq.stringhub.secondaryStream.debug"))
        {
            try 
            {
                FileOutputStream moniDebug = new FileOutputStream("/tmp/moni-" + hubId + ".dat");
                FileOutputStream tcalDebug = new FileOutputStream("/tmp/tcal-" + hubId + ".dat");
                FileOutputStream snDebug   = new FileOutputStream("/tmp/sn-" + hubId + ".dat");
                monitorConsumer.setDebugChannel(moniDebug.getChannel());
                tcalConsumer.setDebugChannel(tcalDebug.getChannel());
                supernovaConsumer.setDebugChannel(snDebug.getChannel());
            }
            catch (FileNotFoundException fnex)
            {
                throw new DAQCompException(fnex.getLocalizedMessage());
            }
        }    

        logger.info("Created secondary stream consumers.");

		try {
			hitsBind = new StreamBinder(nch, sender, "hits");
			moniBind = new StreamBinder(nch, monitorConsumer, "moni");
			snBind   = new StreamBinder(nch, supernovaConsumer, "supernova");
			tcalBind = new StreamBinder(nch, tcalConsumer, "tcal");
			collectorMonitor.setBinders(hitsBind, moniBind, tcalBind, snBind);
		} catch (IOException iox) {
			logger.error("Error creating StreamBinder: " + iox.getMessage());
			iox.printStackTrace();
		} catch (Exception e) {
			throw new DAQCompException("Error in starting S/H - binder allocation fails", e);
		}

		for (BindSource bs : bindSources)
		{
			try {
				hitsBind.register(bs.getHitsSource(), "hits-" + bs.getName());
				moniBind.register(bs.getMoniSource(), "moni-" + bs.getName());
				tcalBind.register(bs.getTcalSource(), "tcal-" + bs.getName());
				snBind.register(bs.getSupernovaSource(), "supernova-" + bs.getName());
			} catch (IOException ioe) {
				logger.error("Couldn't start threads for binder " +
							bs.getName());
				throw new DAQCompException("Could not start DOMs", ioe);
			} catch (Exception e) {
				throw new DAQCompException("Error starting S/H - binder channel register fails", e);
			}
		}

		// start the binders
		logger.info("Starting stream binders.");
		hitsBind.start();
		moniBind.start();
		tcalBind.start();
		snBind.start();

		try
		{
			conn.startProcessing();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new DAQCompException("Couldn't start DOMs", e);
		}		
	}
	
	public void stopping()
            throws DAQCompException
	{
		try
		{
			conn.stopProcessing();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new DAQCompException("Error killing connectors", e);
			// throw new DAQCompException(e.getMessage());
		}

        logger.info("Returning from stop.");
	}

    private void configureTrigger(String configName) throws DAQCompException {
        // Lookup the trigger configuration
        String triggerConfiguration;
        String globalConfigurationFileName = configurationPath + "/" + configName + ".xml";
        try {
            triggerConfiguration = GlobalConfiguration.getTriggerConfig(globalConfigurationFileName);
        } catch (Exception e) {
            logger.error("Error extracting trigger configuration name from global configuraion file.", e);
            throw new DAQCompException("Cannot get trigger configuration name.", e);
        }
        String triggerConfigFileName = configurationPath + "/trigger/" + triggerConfiguration + ".xml";

        // Add triggers to the trigger manager
        List currentTriggers = TriggerBuilder.buildTriggers(triggerConfigFileName, sourceId);
        Iterator triggerIter = currentTriggers.iterator();
        while (triggerIter.hasNext()) {
            ITriggerControl trigger = (ITriggerControl) triggerIter.next();
            trigger.setTriggerHandler(triggerHandler);
        }
        triggerHandler.addTriggers(currentTriggers);
    }

}
