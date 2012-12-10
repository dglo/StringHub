package icecube.daq.dor;

import icecube.daq.util.Leapseconds;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public final class Driver implements IDriver {

    private File driver_root;
    private static final Driver instance = new Driver("/proc/driver/domhub");
    private static final Logger logger = Logger.getLogger(Driver.class);
    private static final boolean DEBUG_ENABLED = logger.isDebugEnabled();

    private Leapseconds leapsecondObj;
    private GPSSynch[] gpsList;

    /**
     * Drivers should be singletons - enforce through protected constructor.
     */
    private Driver(String root) {
	driver_root = new File(root);
	gpsList = new GPSSynch[8];
	for (int i = 0; i < gpsList.length; i++)
	    gpsList[i] = new GPSSynch();

	leapsecondObj = null;
	try {
	    leapsecondObj = Leapseconds.getInstance();
	} catch (IllegalArgumentException e) {
	    // on error creating the leapsecond object
	    // the driver code is setup to operate as if
	    // the leapsecond code never existed in this case
	    // It will however report that the leapsecond
	    // object has expired.  This alert will make it
	    // back to live.
	    System.err.println("leap second object init error: "+e);
	}
    }

    public static Driver getInstance() {
	return instance;
    }

    public double daysTillLeapExpiry() {
	if (leapsecondObj!=null) {
	    return leapsecondObj.daysTillExpiry();
	} else {
	    return -1.0;
	}
    }

    public float getCurrent(int card, int pair) throws IOException {
	String currentText = getProcfileText(makeProcfile(card + "" + pair, "current"));
	Pattern p = Pattern.compile("is ([0-9]+) mA");
	Matcher m = p.matcher(currentText);
	if (m.find()) return Float.parseFloat(m.group(1));
	return 0.0f;
    }

    public boolean power(int card, int pair) throws IOException {
	File file = makeProcfile("" + card + "" + pair, "pwr");
	String info = getProcfileText(file);
	return info.indexOf("on.") != -1;
    }

    public String getProcfileID(int card, int pair, char dom) throws IOException {
	String info = getProcfileText(makeProcfile("" + card + "" + pair + dom, "id"));
	return info.substring(info.length() - 12);
    }

    /**
     * Reset the communications such as to bring back from a hardware timeout.
     * @param card 0 to 7
     * @param pair 0 to 3
     * @param dom 'A' or 'B'
     * @throws IOException
     */
    public void commReset(int card, int pair, char dom) throws IOException
    {
        String cwd = card + "" + pair + dom;
        if (DEBUG_ENABLED) logger.debug("Issuing a communications reset on " + cwd);
        File file = makeProcfile(cwd, "is-communicating");
        FileOutputStream iscomm = new FileOutputStream(file);
	try {
	    iscomm.write("reset\n".getBytes());
	} finally {
	    iscomm.close();
	}
    }

    /**
     * Set blocking / non-blocking mode of the DOR driver
     * @param block if true the driver will be put into blocking mode
     */
    public void setBlocking(boolean block) throws IOException
    {
        File file = makeProcfile("blocking");
        FileOutputStream blockingFile = new FileOutputStream(file);
	try {
	    if (block)
		blockingFile.write("1\n".getBytes());
	    else
		blockingFile.write("0\n".getBytes());
	} finally {
	    blockingFile.close();
	}
    }

    /**
     * Perform soft reset operation on DOM
     * @param card 0 to 7
     * @param pair 0 to 3
     * @param dom 'A' or 'B'
     * @throws IOException when the procfile write fails for some reason
     */
    public void softboot(int card, int pair, char dom) throws IOException
    {
	if (DEBUG_ENABLED) logger.debug("Softbooting " + card + "" + pair + dom);
	File file = makeProcfile(card + "" + pair + dom, "softboot");
	FileOutputStream sb = new FileOutputStream(file);
	try {
	    sb.write("reset\n".getBytes());
	} finally {
	    sb.close();
	}
    }

    /**
     * Reset communications statistics
     * @param card 0 to 7
     * @param pair 0 to 3
     * @param dom 'A' or 'B'
     * @throws IOException when the procfile write fails for some reason
     */
    public void resetComstat(int card, int pair, char dom) throws IOException {
	File file = makeProcfile(card + "" + pair + dom, "comstat");
	FileOutputStream sb = new FileOutputStream(file);
	try {
	    sb.write("reset\n".getBytes());
	} finally {
	    sb.close();
	}
    }

    /**
     * Get communication statistics
     * @param card 0 to 7
     * @param pair 0 to 3
     * @param dom 'A' or 'B'
     * @throws IOException when the procfile write fails for some reason
     */
    public String getComstat(int card, int pair, char dom) throws IOException {
	return getProcfileMultilineText(makeProcfile(card + "" + pair + dom, "comstat"));
    }

    /**
     * Get FPGA register space (except FIFOs)
     * @param card 0 to 7
     * @throws IOException when the procfile write fails for some reason
     */
    public String getFPGARegs(int card) throws IOException {
	return getProcfileMultilineText(makeProcfile(card + "", "fpga"));
    }

    /**
     * Get the list of DOMs that are turned on, communicating, and in iceboot
     * or above (i.e. not in configboot)
     * @return list of DOMChannelInfo structures.
     * @throws IOException
     */
    public LinkedList<DOMChannelInfo> discoverActiveDOMs() throws IOException {
	char[] ab = { 'A', 'B' };
	LinkedList<DOMChannelInfo> channelList = new LinkedList<DOMChannelInfo>();
	for (int card = 0; card < 8; card ++) {
	    File cdir = makeProcfileDir("" + card);
	    if (!cdir.exists()) continue;
	    for (int pair = 0; pair < 4; pair++) {
		File pdir = makeProcfileDir("" + card + "" + pair);
		if (!pdir.exists() || !power(card, pair)) continue;
		if (DEBUG_ENABLED) {
		    logger.debug("Found powered pair on (" + card + ", " + pair + ").");
		}
		for (int dom = 0; dom < 2; dom++) {
		    File ddir = makeProcfileDir("" + card + "" + pair + ab[dom]);
		    if (ddir.exists()) {
			String mbid = getProcfileID(card, pair, ab[dom]);
			if (mbid.matches("[0-9a-f]{12}") && !mbid.equals("000000000000")) {
			    if (DEBUG_ENABLED) {
				logger.debug("Found active DOM on (" + card + ", " + pair + ", " + ab[dom] + ")");
			    }
			    channelList.add(new DOMChannelInfo(mbid, card, pair, ab[dom]));
			}
		    }
		}
	    }
	}
	return channelList;
    }

    public File getTCALFile(int card, int pair, char dom) {
	return makeProcfile("" + card + "" + pair + dom, "tcalib");
    }


    public TimeCalib readTCAL(File tcalFile) throws IOException, InterruptedException {
	RandomAccessFile tcalib = new RandomAccessFile(tcalFile, "rw");
	FileChannel ch = tcalib.getChannel();

	if (DEBUG_ENABLED) logger.debug("Initiating TCAL sequence");
	tcalib.writeBytes("single\n");
	for (int iTry = 0; iTry < 5; iTry++)
	    {
		Thread.sleep(20);
		ByteBuffer buf = ByteBuffer.allocate(292);
		int nr = ch.read(buf);
		if (DEBUG_ENABLED) logger.debug("Read " + nr + " bytes from " + tcalFile.getAbsolutePath());
		if (nr == 292)
		    {
			ch.close();
			tcalib.close();
			buf.flip();
			return new TimeCalib(buf);
		    }
	    }
	ch.close();
	tcalib.close();
	throw new IOException("TCAL read failed.");
    }

    public File getGPSFile(int card) {
	return makeProcfile("" + card, "syncgps");
    }

    public GPSInfo readGPS(File gpsFile) throws GPSException 
    {
	ByteBuffer buf = ByteBuffer.allocate(22);
	
	try
	    {
		RandomAccessFile syncgps = new RandomAccessFile(gpsFile, "r");
		FileChannel ch = syncgps.getChannel();
		int nr = ch.read(buf);
		syncgps.close();
		if (DEBUG_ENABLED) logger.debug("Read " + nr + " bytes from " + gpsFile.getAbsolutePath());
		if (nr == 22)
		    {
			buf.flip();
			GPSInfo gpsinfo = new GPSInfo(buf, leapsecondObj);
			if (DEBUG_ENABLED) logger.debug("GPS read on " + gpsFile.getAbsolutePath() + " - " + gpsinfo);
			return gpsinfo;
		    }
		throw new GPSNotReady(gpsFile.getAbsolutePath(), 0);
	    }
	catch (IOException iox)
	    {
		throw new GPSException(gpsFile.getAbsolutePath(), iox);
	    }
	catch (NumberFormatException nex)
	    {
		throw new GPSException(gpsFile.getAbsolutePath(), nex);
	    }
    }

    private String getProcfileText(File file) throws IOException {
	FileInputStream fis = new FileInputStream(file);
	BufferedReader r = new BufferedReader(new InputStreamReader(fis));
	try {
	    String txt = r.readLine();
	    if (DEBUG_ENABLED) logger.debug(file.getAbsolutePath() + " >> " + txt);
	    return txt;
	} finally {
	    r.close();
	}
    }

    private String getProcfileMultilineText(File file) throws IOException {
	FileInputStream fis = new FileInputStream(file);
	BufferedReader r = new BufferedReader(new InputStreamReader(fis));
	try {
	    String ret = "";
	    String txt;
	    while((txt = r.readLine()) != null) {
		ret += txt+"\n";
		if (DEBUG_ENABLED) logger.debug(file.getAbsolutePath() + " >> " + txt);
	    }
	    return ret;
	} finally {
	    r.close();
	}
    }

    /**
     * Access the DOR card FPGA registers.  They are returned as a dictionary
     * of Key: Value pairs where Key is the
     */
    public HashMap<String, Integer> getFPGARegisters(int card) throws IOException
    {
        HashMap<String, Integer> registerMap = new HashMap<String, Integer>();
        File f = makeProcfile("" + card, "fpga");
        FileInputStream fis = new FileInputStream(f);
	BufferedReader r = new BufferedReader(new InputStreamReader(fis));
	try {
	    Pattern pat = Pattern.compile("([A-Z]+)\\s+0x([0-9a-f]+)");
	    while (true)
		{
		    String txt = r.readLine();
		    if (txt == null || txt.length() == 0) break;
		    Matcher m  = pat.matcher(txt);
		    if (m.matches())
			{
			    String key = m.group(1);
			    long val   = Long.parseLong(m.group(2), 16);
			    registerMap.put(key, new Integer((int) (val & 0xffffffffL)));
			}
		}
	    return registerMap;
	} finally {
	    r.close();
	}
    }

	/**
	 * Manipulate the chain of procfile directories of form
	 * /driver_root/cardX/pairY/domZ/filename
	 * @param cwd - string with card, wirepair, dom encoded - like "40A"
	 * @param filename
	 * @return
	 */
	public File makeProcfile(String cwd, String filename)
    {
		File f = driver_root;
		if (cwd.length() > 0)
			f = new File(driver_root, "card" + cwd.charAt(0));
		if (cwd.length() > 1)
			f = new File(f, "pair" + cwd.charAt(1));
		if (cwd.length() > 2)
			f = new File(f, "dom" + cwd.charAt(2));
		if (filename != null)
			f = new File(f, filename);
		return f;
	}

	/**
	 * This makes a 'top-level' procfile File
	 * @param cwd
	 * @return
	 */
	public File makeProcfile(String filename)
	{
	    return makeProcfile("", filename);
	}

	/**
	 * Make only the directory portion of the procfile
	 * @param cwd card/pair/dom string
	 * @return
	 */
	public File makeProcfileDir(String cwd)
	{
		return makeProcfile(cwd, null);
	}
}

class GPSSynch {
	long last_read_time;
	GPSInfo cached;

	GPSSynch() {
		last_read_time = -1001L;
		cached = null;
	}
}

