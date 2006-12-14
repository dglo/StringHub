package icecube.daq.util;

import java.nio.ByteBuffer;

/**
 * A class to hide the internals of the RAPCal-corrected 
 * timestamps - nominally a long integer holding 0.1 ns ticks.
 * @author kael
 *
 */
public class UTC implements Comparable<UTC> {
	
	private long tick10;
	
	public UTC() {
		tick10 = 0L;
	}
	
	public UTC(long time) {
		tick10 = time;
	}

	public UTC(ByteBuffer buf) {
		tick10 = buf.getLong();
	}
	
	public String toString() {
		return String.valueOf(tick10);
	}
	
	/**
	 * Return the difference time in seconds between two UTCs (this - other)
	 * @param utc - the other UTC
	 * @return difference in seconds
	 */
	public static double subtract(UTC utc1, UTC utc0) {
		return 1.0e-10 * (utc1.tick10 - utc0.tick10);
	}
	
	public static UTC add(UTC utc, double t) {
		return new UTC(utc.tick10 + (long) (1.0e+10 * t));
	}
	
	public static UTC add(UTC utc0, UTC utc1) {
		return new UTC(utc0.tick10 + utc1.tick10);
	}

	/**
	 * Write to a byte buffer.
	 * @param buf
	 */
	public void toBuf(ByteBuffer buf) {
		buf.putLong(tick10);
	}

	public int compareTo(UTC o) {
		if (this.tick10 < o.tick10) 
			return -1;
		else if (this.tick10 > o.tick10)
			return 1;
		else
			return 0;
	}

}
