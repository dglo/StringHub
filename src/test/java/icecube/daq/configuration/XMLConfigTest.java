package icecube.daq.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import icecube.daq.common.MockAppender;
import icecube.daq.domapp.AtwdChipSelect;
import icecube.daq.domapp.DOMConfiguration;
import icecube.daq.domapp.LocalCoincidenceConfiguration;
import icecube.daq.domapp.TriggerMode;

import java.io.InputStream;

import junit.framework.JUnit4TestAdapter;

import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class XMLConfigTest 
{
	private static final MockAppender appender = new MockAppender();

	private XMLConfig xmlConfig;
	private DOMConfiguration config, config2;
	
	/**
	 * This should make the tests JUnit 3.8 compatible
	 * @return
	 */
	public static junit.framework.Test suite()
	{
		return new JUnit4TestAdapter(XMLConfigTest.class);
	}
		
	@BeforeClass public static void initialize() throws Exception
	{
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure(appender);
	}
	
	@Before public void setUp() throws Exception
	{
        InputStream xmlIn = XMLConfig.class.getResourceAsStream("sample-config.xml");
		xmlConfig = new XMLConfig();
		xmlConfig.parseXMLConfig(xmlIn);
		config = xmlConfig.getDOMConfig("57bc3f3a220d");
		config2 = xmlConfig.getDOMConfig("9a0744bca158");
	}

	@Test public void testGotDOMConfigs() 
	{
		// look for the dom configs declared
		assertNotNull(config);
		assertNotNull(config2);
	}
	
	@Test public void testFormat()
	{
        assertFalse(config.isDeltaCompressionEnabled());
        assertEquals((short) 128, config.getEngineeringFormat().atwdSamples(0));
        assertEquals((short)  32, config.getEngineeringFormat().atwdSamples(1));
        assertEquals((short)  32, config.getEngineeringFormat().atwdSamples(2));
        assertEquals((short)   0, config.getEngineeringFormat().atwdSamples(3));
        assertEquals((short) 200, config.getEngineeringFormat().fadcSamples());
	}
	
	@Test public void testTrigger()
	{
        assertEquals(TriggerMode.MPE, config.getTriggerMode());
        assertEquals((short) 665, config.getDAC(8));
        assertEquals((short) 569, config.getDAC(9));
	}
	
	@Test public void testLocalCoincidence()
	{
        assertEquals(1250, config.getLC().getPreTrigger());
        assertEquals(1450, config.getLC().getPostTrigger());
        assertEquals((byte) 3, config.getLC().getSpan());
        assertEquals(LocalCoincidenceConfiguration.Source.MPE, config.getLC().getSource());
	}
	
	@Test public void testMonitorIntervals()
	{
        assertEquals(168000000, config.getHardwareMonitorInterval());
        assertEquals(60000000, config.getFastMonitorInterval());
	}
	
	@Test public void testIceTopMinBias()
	{
	    assertTrue(config.isMinBiasEnabled());
	}
	
	@Test public void testAtwdChipSelect()
	{
	    assertEquals(AtwdChipSelect.ATWD_A, config.getAtwdChipSelect());
	}
	
	@Test public void testAtwdChargeStamp()
	{
	    assertTrue(config.isAtwdChargeStamp());
	    assertEquals(2, (int) config.getChargeStampChannel());
	    assertTrue(config2.isAtwdChargeStamp());
	    assertTrue(config2.isAutoRangeChargeStamp());
	    assertEquals(2, (int) config2.getChargeStampChannel());
	}
	@Test public void testPedestalSubtract()
	{
	    assertFalse(config.getPedestalSubtraction());
	    assertTrue(config2.getPedestalSubtraction());
	    assertEquals(140, config2.getAveragePedestal(0));
        assertEquals(120, config2.getAveragePedestal(1));
        assertEquals(130, config2.getAveragePedestal(2));
        assertEquals(142, config2.getAveragePedestal(3));
        assertEquals(122, config2.getAveragePedestal(4));
        assertEquals(132, config2.getAveragePedestal(5));
	    
	}
}
