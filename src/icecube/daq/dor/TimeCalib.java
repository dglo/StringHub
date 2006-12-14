package icecube.daq.dor;

import icecube.daq.util.UTC;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.log4j.Logger;

public class TimeCalib {
	
	private int bytes;
	private int flags;
	private long dorTx, dorRx;
	private short[] dorWaveform;
	private long domRx, domTx;
	private short[] domWaveform;
	
	private static final Logger logger = Logger.getLogger(TimeCalib.class);
	
	public TimeCalib(ByteBuffer buf) {
		buf.order(ByteOrder.LITTLE_ENDIAN);
		bytes = buf.getShort();
		flags = buf.getShort();
		dorTx = buf.getLong();
		dorRx = buf.getLong();
		dorWaveform = new short[64];
		for (int i = 0; i < 64; i++) dorWaveform[i] = buf.getShort();
		domRx = buf.getLong();
		domTx = buf.getLong();
		domWaveform = new short[64];
		for (int i = 0; i < 64; i++) domWaveform[i] = buf.getShort();
		logger.debug("Decode TCAL record - len: " + bytes + " - flags: " + flags + " dorTx: " + dorTx);
	}
	
	/**
	 * Get the DOR TCAL transmit time
	 * @return UTC object
	 */
	public UTC getDorTx() {
		return new UTC(500L * dorTx);
	}
	
	public UTC getDorRx() {
		return new UTC(500L * dorRx);
	}
	
	public UTC getDomTx() {
		return new UTC(250L * domTx);
	}
	
	public UTC getDomRx() {
		return new UTC(250L * domRx);
	}
	
	public short[] getDorWaveform() {
		return dorWaveform;
	}
	
	public short[] getDomWaveform() {
		return domWaveform;
	}
	
}
