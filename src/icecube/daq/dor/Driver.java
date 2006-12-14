package icecube.daq.dor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;

import org.apache.log4j.Logger;

public class Driver implements IDriver {

	private File driver_root;
	private static final Driver instance = new Driver("/proc/driver/domhub");
	private static final Logger logger = Logger.getLogger(Driver.class);

	private GPSSynch[] gpsList;
	
	/**
	 * Drivers should be singletons - enforce through protected constructor.
	 */
	private Driver(String root) { 
		driver_root = new File(root);
		gpsList = new GPSSynch[8];
		for (int i = 0; i < gpsList.length; i++) 
			gpsList[i] = new GPSSynch();
	}
	
	public static Driver getInstance() {
		return instance;
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
	
	public LinkedList<DOMChannelInfo> discoverActiveDOMs() throws IOException {
		char ab[] = { 'A', 'B' };
		LinkedList<DOMChannelInfo> channelList = new LinkedList<DOMChannelInfo>();
		for (int card = 0; card < 8; card ++) {
			File cdir = makeProcfile("" + card);
			if (!cdir.exists()) continue;
			for (int pair = 0; pair < 4; pair++) {
				File pdir = makeProcfile("" + card + "" + pair);
				if (!pdir.exists() || !power(card, pair)) continue;
				logger.info("Found powered pair on (" + card + ", " + pair + ").");
				for (int dom = 0; dom < 2; dom++) {
					File ddir = makeProcfile("" + card + "" + pair + ab[dom]);
					if (ddir.exists()) {
						String mbid = getProcfileID(card, pair, ab[dom]);
						if (mbid.matches("[0-9a-f]{12}") && !mbid.equals("000000000000")) {
							logger.info("Found active DOM on (" + card + ", " + pair + ", " + ab[dom] + ")");
							channelList.add(new DOMChannelInfo(mbid, card, pair, ab[dom]));
						}
					}
				}
			}
		}
		return channelList;
	}
	
	public TimeCalib readTCAL(int card, int pair, char dom) throws IOException, InterruptedException {
		File file = makeProcfile("" + card + "" + pair + dom, "tcalib");
		RandomAccessFile tcalib = new RandomAccessFile(file, "rw");
		
		tcalib.writeBytes("single\n");
		for (int iTry = 0; iTry < 5; iTry++)
		{
			Thread.sleep(20);
			FileChannel ch = tcalib.getChannel();
			ByteBuffer buf = ByteBuffer.allocate(292);
			int nr = ch.read(buf);	
			logger.debug("Read " + nr + " bytes from " + file.getAbsolutePath());
			if (nr == 292)
			{
				ch.close();
				tcalib.close();
				buf.flip();
				return new TimeCalib(buf);
			}
		}
		throw new IOException("TCAL read failed.");
	}
	
	public GPSInfo readGPS(int card) throws GPSException {
		
		// Try to enforce not reading the GPS procfile more than once per second
		GPSSynch gps = gpsList[card];
		long current = System.currentTimeMillis();
		if (System.currentTimeMillis() - gps.last_read_time < 1001) return gps.cached;
		
		ByteBuffer buf = ByteBuffer.allocate(22);
		File file = makeProcfile("" + card, "syncgps");
		try {
			RandomAccessFile syncgps = new RandomAccessFile(file, "r");
			FileChannel ch = syncgps.getChannel();
			int nr = ch.read(buf);
			logger.debug("Read " + nr + " bytes from " + file.getAbsolutePath());
			if (nr != 22) throw new GPSException(file.getAbsolutePath());
			ch.close();
			syncgps.close();
			buf.flip();
			GPSInfo gpsinfo = new GPSInfo(buf);
			gps.cached = gpsinfo;
			gps.last_read_time = current;
			logger.info("GPS read on " + file.getAbsolutePath() + " - " + gpsinfo);
			return gpsinfo;
		} 
		catch (IOException iox) 
		{
			throw new GPSException(file.getAbsolutePath(), iox);
		} 
		catch (NumberFormatException nex)
		{
			throw new GPSException(file.getAbsolutePath(), nex);
		}
	}
	
	private String getProcfileText(File file) throws IOException {
		BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		String txt = r.readLine();
		logger.debug(file.getAbsolutePath() + " >> " + txt);
		return txt;
	}
	
	/**
	 * Manipulate the chain of procfile directories of form 
	 * /driver_root/cardX/pairY/domZ/filename
	 * @param cwd - string with card, wirepair, dom encoded - like "40A"
	 * @param filename
	 * @return
	 */
	public File makeProcfile(String cwd, String filename) {
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
	
	public File makeProcfile(String cwd) {
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
