package icecube.daq.domapp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

public class IcebootInterface extends DOMIO
{
    private static final Logger logger = Logger.getLogger(IcebootInterface.class);
    
    public IcebootInterface(int card, int pair, char dom) throws FileNotFoundException
    {
        super(card, pair, dom);
    }
    
    public String sendCommand(String cmd) throws IOException
    {
        return sendCommand(cmd, "> \n");
    }
    
    /**
     * Higher-level interface to sending iceboot commands. This method is normally called with an expect
     * string of "> \n" which 
     * @param cmd
     * @param expect
     * @return 
     * @throws IOException
     */
    public String sendCommand(String cmd, String expect) throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.put(cmd.getBytes());
        buf.put("\r\n".getBytes()).flip();
        send(buf);
        if (logger.isDebugEnabled()) logger.debug("Sending: " + cmd);
        while (true)
        {
            ByteBuffer ret = recv();
            byte[] bytearray = new byte[ret.remaining()];
            ret.get(bytearray);
            String fragment = new String(bytearray);
            if (logger.isDebugEnabled()) logger.debug("Received: " + fragment);
            if (fragment.contains(cmd)) break;
        }
        if (expect == null) return "";
        if (logger.isDebugEnabled()) logger.debug("Echoback from iceboot received - expecting ... " + expect);
        StringBuffer txt = new StringBuffer();
        while (true)
        {
            ByteBuffer ret = recv();
            byte[] bytearray = new byte[ret.remaining()];
            ret.get(bytearray);
            String fragment = new String(bytearray);
            if (fragment.contains(expect)) break;
            txt.append(fragment);
        }
        return txt.toString().trim();
    }
    
    public String getMainboardId() throws IOException
    {
        return sendCommand("domid type");
    }
    
    public void powerOnFlasherboard() throws IOException
    {
        sendCommand("enableFBminY");
    }
    
    public void powerOffFlasherboard() throws IOException
    {
        sendCommand("disableFB");
    }
    
    /**
     * Read the DOM FPGA system time at the STF address
     * @return 48-bit DOM clock in 25 ns tick units (approx.)
     * @throws IOException
     */
    public long getDOMClockSTF() throws IOException
    {
        String[] w = sendCommand("$90081040 @ $90081044 @ . . drop drop").split(" ");
        long hiByte = Integer.parseInt(w[0]);
        long loByte = Integer.parseInt(w[1]);
        if (hiByte < 0L) hiByte += (1L << 32);
        if (loByte < 0L) loByte += (1L << 32);
        return (hiByte << 32) | loByte;
    }

    /**
     * Loads the DOMApp FPGA image.
     * @throws IOException
     */
    public void loadDOMAppSBI() throws IOException
    {
        sendCommand("s\" domapp.sbi.gz\" find if gunzip fpga endif");
    }
}
