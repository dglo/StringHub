package icecube.daq.domapp;

import icecube.daq.domapp.LocalCoincidenceConfiguration.Source;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.apache.log4j.Logger;

public class DOMApp implements IDOMApp
{

    private static Logger logger = Logger.getLogger(DOMApp.class);
    private DOMIO         devIO;
    private ByteBuffer    msgBuffer;
    private ByteBuffer    msgBufferOut;

    // prebuilt query commands
    private byte[] dataMsgArray;
    private byte[] moniMsgArray;
    private byte[] snMsgArray;
    private byte[] intervalMsgArray;

    private static final int HEADER_LENGTH = 8;

    /**
     * Create a new DOMApp connection object for DOR channel provided
     *
     * @param card
     * @param pair
     * @param dom
     */
    public DOMApp(int card, int pair, char dom) throws IOException
    {
        devIO = new DOMIO(card, pair, dom);
        msgBuffer = ByteBuffer.allocate(4092);
        msgBufferOut = ByteBuffer.allocate(4092);

	preBuildMessages();
    }

    protected void buildMessage(MessageType m_type, ByteBuffer tmpBuffer) {
	tmpBuffer.clear();
	tmpBuffer.put(m_type.getFacility()).put(m_type.getSubtype());
	// put the length of the payload ( 0 in this case )
	tmpBuffer.putShort((short)0);
	// 16 bites of reserved
	tmpBuffer.putShort((short)0);
	// message id
	tmpBuffer.put((byte)0);
	// status byte
	tmpBuffer.put((byte)0);
	// no payload
	tmpBuffer.flip();
    }

    protected void preBuildMessages()
    {
	// allocate space for the messages we are about
	// to pre-build
	dataMsgArray = new byte[HEADER_LENGTH];
	moniMsgArray = new byte[HEADER_LENGTH];
	snMsgArray = new byte[HEADER_LENGTH];
	intervalMsgArray = new byte[HEADER_LENGTH];
	ByteBuffer tmpBuffer = ByteBuffer.allocate(HEADER_LENGTH);

	// GET_INTERVAL message
	// --------------------
	buildMessage(MessageType.GET_INTERVAL, tmpBuffer);
	tmpBuffer.get(intervalMsgArray, 0, intervalMsgArray.length);

	// Data message
	// ------------
	buildMessage(MessageType.GET_DATA, tmpBuffer);
	tmpBuffer.get(dataMsgArray, 0, dataMsgArray.length);

	// moni message
	// ------------
	buildMessage(MessageType.GET_MONI, tmpBuffer);
	tmpBuffer.get(moniMsgArray, 0, moniMsgArray.length);

	// supernova message
	// -----------------
	buildMessage(MessageType.GET_SN_DATA, tmpBuffer);
	tmpBuffer.get(snMsgArray, 0, snMsgArray.length);
    }


    /*
     * send a PRE-BUILT message out to query the dom
     *
     * Mainly this should be used for high frequency messages
     * like the 'data', 'moni', and 'supernova' messages
     *
     */
    protected ByteBuffer sendMessagePreBuilt(MessageType type, byte[] query) throws MessageException
    {

        try
	    {
		devIO.send(query);

		recvMessage(msgBufferOut);

		// recvMessage returns a message
		// with the header, skip that
		msgBufferOut.position(HEADER_LENGTH);
		return msgBufferOut.slice();
	    }
        catch (IOException e)
	    {
		throw new MessageException(type, e);
	    }
    }

    @Override
    public void close()
    {
        devIO.close();
    }

    @Override
    public void changeFlasherSettings(
            short brightness,
            short width,
            short delay,
            short mask,
            short rate) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(10);
        buf.putShort(brightness).putShort(width).putShort(delay).putShort(mask).putShort(rate).flip();
        sendMessage(MessageType.CHANGE_FB_SETTINGS, buf);
    }

    @Override
    public void beginFlasherRun(short brightness, short width, short delay, short mask, short rate)
            throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(10);
        buf.putShort(brightness).putShort(width).putShort(delay).putShort(mask).putShort(rate).flip();
        sendMessage(MessageType.BEGIN_FB_RUN, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#beginRun()
     */
    @Override
    public void beginRun() throws MessageException
    {
        sendMessage(MessageType.BEGIN_RUN);
    }

    @Override
    public void collectPedestals(int nAtwd0, int nAtwd1, int nFadc, Integer... avgPedestals) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(24);
        buf.putInt(nAtwd0).putInt(nAtwd1).putInt(nFadc);
        if (avgPedestals.length == 6)
        {
            buf.putShort(avgPedestals[0].shortValue());
            buf.putShort(avgPedestals[1].shortValue());
            buf.putShort(avgPedestals[2].shortValue());
            buf.putShort(avgPedestals[3].shortValue());
            buf.putShort(avgPedestals[4].shortValue());
            buf.putShort(avgPedestals[5].shortValue());
        }
        buf.flip();
        sendMessage(MessageType.COLLECT_PEDESTALS, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#disableHV()
     */
    @Override
    public void disableHV() throws MessageException
    {
        sendMessage(MessageType.DISABLE_HV);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#disableSupernova()
     */
    @Override
    public void disableSupernova() throws MessageException
    {
        sendMessage(MessageType.DISABLE_SN);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#enableHV()
     */
    @Override
    public void enableHV() throws MessageException
    {
        sendMessage(MessageType.ENABLE_HV);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#enableSupernova(int, boolean)
     */
    @Override
    public void enableSupernova(int deadtime, boolean speDisc) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(5);
        byte mode = 1;
        if (speDisc) mode = 0;
        buf.putInt(deadtime).put(mode).flip();
        sendMessage(MessageType.ENABLE_SN, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#endRun()
     */
    @Override
    public void endRun() throws MessageException
    {
        sendMessage(MessageType.END_RUN);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#getData()
     */
    @Override
    public ByteBuffer getData() throws MessageException
    {
        return sendMessagePreBuilt(MessageType.GET_DATA,
				   dataMsgArray);
    }

    /**
     * Pack multiple messages to get data
     */
    @Override
    public ArrayList<ByteBuffer> getData(int n) throws MessageException
    {
        ByteBuffer buf = (ByteBuffer) msgBuffer.clear();
        for (int i = 0; i < n; i++)
        {
            buf.put(MessageType.GET_DATA.getFacility());
            buf.put(MessageType.GET_DATA.getSubtype());
            buf.putShort((short) 0).putShort((short) 0);
            buf.put((byte) 0).put((byte) 0);
        }

        buf.flip();
        logger.debug("Sending multimessage request.");
        try
        {
            devIO.send(buf);
        }
        catch (IOException e)
        {
            throw new MessageException(MessageType.GET_DATA, e);
        }

        ArrayList<ByteBuffer> outC = new ArrayList<ByteBuffer>();

        for (int i = 0; i < n; i++)
        {
            try
            {
                Thread.yield();
                ByteBuffer out = devIO.recv();
                if (logger.isDebugEnabled())
                    logger.debug("Received part " + i + " of multimessage.");
                int status = out.get(7);
                if (status != 1) throw new MessageException(
                        MessageType.GET_DATA, out.get(0), out.get(1),
                        status);
                if (out.remaining() > 8)
                {
                    ByteBuffer x = ByteBuffer.allocate(out.remaining() - 8);
                    out.position(8);
                    x.put(out);
                    outC.add((ByteBuffer) x.flip());
                }
            }
            catch (IOException e)
            {
                throw new MessageException(MessageType.GET_DATA, e);
            }
        }
        return outC;
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#getMainboardID()
     */
    @Override
    public String getMainboardID() throws MessageException
    {
        ByteBuffer buf = sendMessage(MessageType.GET_DOM_ID);
        int len = buf.remaining();
        byte[] mbid = new byte[len];
        buf.get(mbid);
        return new String(mbid);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#getMoni()
     */
    @Override
    public ByteBuffer getMoni() throws MessageException
    {
        return sendMessagePreBuilt(MessageType.GET_MONI,
				   moniMsgArray);
    }

    public MuxState getMux() throws MessageException
    {
        ByteBuffer buf = sendMessage(MessageType.GET_MUX_CH);
        int muxCh = buf.get();
        switch (muxCh)
        {
        case -1: return MuxState.OFF;
        case 0: return MuxState.OSC_OUTPUT;
        case 1: return MuxState.SQUARE_40MHZ;
        case 2: return MuxState.LED_CURRENT;
        case 3: return MuxState.FB_CURRENT;
        case 4: return MuxState.UPPER_LC;
        case 5: return MuxState.LOWER_LC;
        case 6: return MuxState.COMM_ADC_INPUT;
        case 7: return MuxState.FE_PULSER;
        default: throw new MessageException(MessageType.GET_MUX_CH,
                new IllegalArgumentException(muxCh + " is not a valid MUXer channel"));
        }
    }
    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#getPulserRate()
     */
    @Override
    public short getPulserRate() throws MessageException
    {
        // TODO - implement
        return (short) 0;
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#getRelease()
     */
    @Override
    public String getRelease() throws MessageException
    {
        ByteBuffer buf = sendMessage(MessageType.GET_DOMAPP_RELEASE);
        int len = buf.remaining();
        byte[] rel = new byte[len];
        buf.get(rel);
        return new String(rel);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#getScalerDeadtime()
     */
    @Override
    public int getScalerDeadtime() throws MessageException
    {
        // TODO - implement
        return 0;
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#getSupernova()
     */
    @Override
    public ByteBuffer getSupernova() throws MessageException
    {
        return sendMessagePreBuilt(MessageType.GET_SN_DATA,
				   snMsgArray);
    }

    /**
     * Enable charge stamp histogramming.
     * These histograms will be emitted in the monitoring records.  The histogram
     * length is 127 bytes so the prescale argument is used to set the range.
     * @param interval interval in clock ticks (&gt; 40M) or seconds (&lt; 40M)
     * @param prescale divisor for the chargestamp.
     * @throws MessageException
     */
    @Override
    public void histoChargeStamp(int interval, short prescale) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(6);
        buf.putInt(interval);
        buf.putShort(prescale);
        buf.flip();
        sendMessage(MessageType.HISTO_CHARGE_STAMPS, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#pulserOff()
     */
    @Override
    public void pulserOff() throws MessageException
    {
        sendMessage(MessageType.SET_PULSER_OFF);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#pulserOn()
     */
    @Override
    public void pulserOn() throws MessageException
    {
        sendMessage(MessageType.SET_PULSER_ON);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#queryHV()
     */
    @Override
    public short[] queryHV() throws MessageException
    {
        ByteBuffer buf = sendMessage(MessageType.QUERY_HV);
        short[] hv = new short[2];
        hv[0] = buf.getShort();
        hv[1] = buf.getShort();
        return hv;
    }

    /**
     * Sends a message with no data payload.
     *
     * @param type
     *            the message type and subtype
     * @return the return data payload.
     */
    protected ByteBuffer sendMessage(MessageType type) throws MessageException
    {
        return sendMessage(type, ByteBuffer.allocate(0));
    }


    /**
     *
     * Sends a query for one second of data
     *
     */
    @Override
    public void getInterval() throws MessageException
    {
	sendMessagePreBuilt(MessageType.GET_INTERVAL,
			    intervalMsgArray);
    }

    @Override
    public ByteBuffer recvMessage(ByteBuffer recvBuf) throws MessageException
    {
        recvBuf.clear();

        try
        {
            // Loop on receive - allow partial receives
            while (recvBuf.position() < 8 || recvBuf.position() < recvBuf.getShort(2) + 8)
            {
                Thread.yield();
                ByteBuffer out = devIO.recv();
                recvBuf.put(out);
            }
            recvBuf.flip();
            int status = recvBuf.get(7);
	    if (status != 1) {
		throw new MessageException(MessageType.GET_DATA,
					   recvBuf.get(0), recvBuf.get(1),
					   status);
	    }

            return recvBuf;
        }
        catch (IOException e)
        {
            throw new MessageException(MessageType.GET_DATA, e);
        }
    }


    /**
     * Send DOMApp message and receive response
     *
     * @param type -
     *            the message type / subtype
     * @param in -
     *            ByteBuffer object holding data to be appended to message
     * @return ByteBuffer holding the message return data payload
     */
    protected ByteBuffer sendMessage(MessageType type, ByteBuffer in) throws MessageException
    {
        ByteBuffer buf = (ByteBuffer) msgBuffer.clear();
        // Put the message id and subtypes in 1st two bytes
        buf.put(type.getFacility()).put(type.getSubtype());
        // Then comes length of data payload
        buf.putShort((short) in.remaining());
        // Then comes 16-bits of reserved
        buf.putShort((short) 0);
        // Then comes message ID (?? - zero)
        buf.put((byte) 0);
        // Then comes the status byte - should be zero going down and return 1
        buf.put((byte) 0);
        // Tack on the data payload
        buf.put(in);
        buf.flip();
        if (logger.isDebugEnabled())
            logger.debug("sendMessage [" + type.name() + "]");

        msgBufferOut.clear();

        try
        {
            devIO.send(buf);

            // Loop on receive - allow partial receives
            while (msgBufferOut.position() < 8 || msgBufferOut.position() < msgBufferOut.getShort(2) + 8)
            {
                Thread.yield();
                ByteBuffer out = devIO.recv();
                msgBufferOut.put(out);
            }
            msgBufferOut.flip();
            int status = msgBufferOut.get(7);
            msgBufferOut.position(8);
            if (!(type.equals(msgBufferOut.get(0), msgBufferOut.get(1)) && status == 1))
                throw new MessageException(type, msgBufferOut.get(0), msgBufferOut.get(1), status);
            return msgBufferOut.slice();
        }
        catch (IOException e)
        {
            throw new MessageException(type, e);
        }
    }

    @Override
    public void setAtwdReadout(AtwdChipSelect csel) throws MessageException
    {
        byte bsel = (byte) csel.ordinal();
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put(bsel);
        buf.flip();
        sendMessage(MessageType.SELECT_ATWD, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setCableLengths(short[], short[])
     */
    @Override
    public void setCableLengths(short[] up, short[] dn) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(16);
        for (int i = 0; i < 4; i++)
            buf.putShort(up[i]);
        for (int i = 0; i < 4; i++)
            buf.putShort(dn[i]);
        buf.flip();
        sendMessage(MessageType.SET_LC_CABLELEN, buf);
    }

    @Override
    public void setChargeStampType(boolean fADC, boolean autoRange, byte chan) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(3);
        buf.put((byte) (fADC ? 1 : 0));
        buf.put((byte) (autoRange ? 0 : 1));
        buf.put(chan);
        buf.flip();
        sendMessage(MessageType.SET_CHARGE_STAMP_TYPE, buf);
    }
    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setDeltaCompressionFormat()
     */
    @Override
    public void setDeltaCompressionFormat() throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put((byte) 2).flip();
        sendMessage(MessageType.SET_COMPRESSION_MODE, buf);
        buf.rewind();
        sendMessage(MessageType.SET_DATA_FORMAT, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setEngineeringFormat(ic3.daq.domapp.EngineeringRecordFormat)
     */
    @Override
    public void setEngineeringFormat(EngineeringRecordFormat fmt) throws MessageException
    {
        byte[] enc = fmt.encode();
        ByteBuffer buf = ByteBuffer.allocate(3);
        buf.put((byte) 0).flip();
        sendMessage(MessageType.SET_COMPRESSION_MODE, buf);
        buf.rewind();
        sendMessage(MessageType.SET_DATA_FORMAT, buf);
        buf.clear();
        buf.put(enc[0]).put(enc[1]).put(enc[2]).flip();
        if (logger.isDebugEnabled())
            logger.debug("Setting engineering format bytes to (" + enc[0] + ", " + enc[1] + ", " + enc[2] + ").");
        sendMessage(MessageType.SET_ENG_FORMAT, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setHV(short)
     */
    @Override
    public void setHV(short dac) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(2);
        buf.putShort(dac).flip();
        sendMessage(MessageType.SET_HV, buf);
    }

    @Override
    public void setLBMDepth(LBMDepth depth) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put((byte) (depth.ordinal()+8)).flip();
        try
        {
            sendMessage(MessageType.SET_LBM_DEPTH, buf);
        }
        catch (MessageException mex)
        {
            /*
             * work-around to ignore messages which fail here - domapp forgot
             * to set the success to OK on this message!
             */
        }
    }

    public LBMDepth getLBMDepth() throws MessageException
    {
        ByteBuffer buf = sendMessage(MessageType.GET_LBM_DEPTH);
        int length = buf.getInt();
        switch (length)
        {
        case 0x000100: return LBMDepth.LBM_256;
        case 0x000200: return LBMDepth.LBM_512;
        case 0x000400: return LBMDepth.LBM_1K;
        case 0x000800: return LBMDepth.LBM_2K;
        case 0x001000: return LBMDepth.LBM_4K;
        case 0x002000: return LBMDepth.LBM_8K;
        case 0x004000: return LBMDepth.LBM_16K;
        case 0x008000: return LBMDepth.LBM_32K;
        case 0x010000: return LBMDepth.LBM_64K;
        case 0x020000: return LBMDepth.LBM_128K;
        case 0x040000: return LBMDepth.LBM_256K;
        case 0x080000: return LBMDepth.LBM_512K;
        case 0x100000: return LBMDepth.LBM_1M;
        case 0x200000: return LBMDepth.LBM_2M;
        case 0x400000: return LBMDepth.LBM_4M;
        case 0x800000: return LBMDepth.LBM_8M;
        default:
            throw new MessageException(MessageType.GET_LBM_DEPTH, new IllegalArgumentException());
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setLCMode(ic3.daq.domapp.LocalCoincidenceConfiguration.RxMode)
     */
    @Override
    public void setLCMode(LocalCoincidenceConfiguration.RxMode mode) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put(mode.asByte()).flip();
        sendMessage(MessageType.SET_LC_MODE, buf);
    }

    @Override
    public void setLCSource(Source src) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put(src.asByte()).flip();
        sendMessage(MessageType.SET_LC_SRC, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setLCSpan(byte)
     */
    @Override
    public void setLCSpan(byte span) throws MessageException
    {
        // TODO Auto-generated method stub
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put(span).flip();
        sendMessage(MessageType.SET_LC_SPAN, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setLCTx(ic3.daq.domapp.LocalCoincidenceConfiguration.TxMode)
     */
    @Override
    public void setLCTx(LocalCoincidenceConfiguration.TxMode mode) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put(mode.asByte()).flip();
        sendMessage(MessageType.SET_LC_TX, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setLCType(ic3.daq.domapp.LocalCoincidenceConfiguration.Type)
     */
    @Override
    public void setLCType(LocalCoincidenceConfiguration.Type type) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put(type.asByte()).flip();
        sendMessage(MessageType.SET_LC_TYPE, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setLCWindow(int, int)
     */
    @Override
    public void setLCWindow(int pre, int post) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putInt(pre).putInt(post).flip();
        sendMessage(MessageType.SET_LC_WIN, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setMoniIntervals(int, int)
     */
    @Override
    public void setMoniIntervals(int hw, int config) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putInt(hw).putInt(config).flip();
        sendMessage(MessageType.SET_MONI_IVAL, buf);
    }

    @Override
    public void setMoniIntervals(int hw, int config, int fast) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(12);
        buf.putInt(hw).putInt(config).putInt(fast).flip();
        sendMessage(MessageType.SET_MONI_IVAL, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setMux(ic3.daq.domapp.MuxState)
     */
    @Override
    public void setMux(MuxState mode) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put(mode.getValue()).flip();
        sendMessage(MessageType.MUX_SELECT, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setPulserRate(short)
     */
    @Override
    public void setPulserRate(short rate) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(2);
        buf.putShort(rate).flip();
        sendMessage(MessageType.SET_PULSER_RATE, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setScalerDeadtime(int)
     */
    @Override
    public void setScalerDeadtime(int deadtime) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(deadtime).flip();
        sendMessage(MessageType.SET_SCALER_DEADTIME, buf);
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#setTriggerMode(ic3.daq.domapp.TriggerMode)
     */
    @Override
    public void setTriggerMode(TriggerMode mode) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put(mode.getValue()).flip();
        sendMessage(MessageType.SET_TRIG_MODE, buf);
    }

    @Override
    public boolean isRunningDOMApp() throws IOException, InterruptedException
    {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.put(new byte[] { 1, 10, 0, 0, 13, 10, 0, 0} );
        buf.flip();
        devIO.send(buf);
        ByteBuffer ack = ByteBuffer.allocate(34);
        while (ack.position() < 20) ack.put(devIO.recv());
        // if the 5th byte is an 'E'
        StringBuilder debugTxt = new StringBuilder("DOMApp detector returns");
        for (int i = 0; i < 8; i++) {
            int b = ack.get(i);
            if (b < 0) b += 256;
            debugTxt.append(String.format(" %02x", b));
        }
        logger.debug(debugTxt);
        if (ack.get(4) != (byte) 0x45) return true;
        // finish up reading iceboot response
        while (ack.position() < 34) ack.put(devIO.recv());
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#transitionToDOMApp()
     */
    @Override
    public boolean transitionToDOMApp() throws IOException, InterruptedException
    {
        // Issue a clear - something gets out-of-sorts in the iceboot
        // command decoder
        String status = talkToIceboot("s\" domapp.sbi.gz\" find if gunzip fpga endif . set-comm-params");
        if (logger.isDebugEnabled()) {
            logger.debug("FPGA reload returns: " + status);
        }
        // Exec DOMApp & wait for "DOMAPP READY" message from DOMApp
        String expect = "DOMAPP READY";
        boolean reticence = Boolean.getBoolean("icecube.daq.domapp.reticence");
        if (reticence) expect = null;
        talkToIceboot("s\" domapp.gz\" find if gunzip exec endif", expect);
        if (reticence) Thread.sleep(8500);
        return true;
    }

    private String talkToIceboot(String cmd) throws IOException
    {
        return talkToIceboot(cmd, "> \n");
    }

    private String talkToIceboot(String cmd, String expect) throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.put(cmd.getBytes());
        buf.put("\r\n".getBytes()).flip();
        devIO.send(buf);
        if (logger.isDebugEnabled()) logger.debug("Sending: " + cmd);
        while (true)
        {
            ByteBuffer ret = devIO.recv();
            byte[] bytearray = new byte[ret.remaining()];
            ret.get(bytearray);
            String fragment = new String(bytearray);
            if (logger.isDebugEnabled()) logger.debug("Received: " + fragment);
            if (fragment.contains(cmd)) break;
        }
        if (expect == null) return "";
        if (logger.isDebugEnabled()) logger.debug("Echoback from iceboot received - expecting ... " + expect);
        StringBuilder txt = new StringBuilder();
        while (true)
        {
            ByteBuffer ret = devIO.recv();
            byte[] bytearray = new byte[ret.remaining()];
            ret.get(bytearray);
            String fragment = new String(bytearray);
            if (fragment.contains(expect)) break;
            txt.append(fragment);
        }
        return txt.toString();
    }

    /*
     * (non-Javadoc)
     *
     * @see ic3.daq.domapp.IDOMApp#writeDAC(byte, short)
     */
    @Override
    public void writeDAC(byte dac, short val) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.put(dac).put((byte) 0).putShort(val).flip();
        sendMessage(MessageType.WRITE_DAC, buf);
    }

    @Override
    public void disableMinBias() throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.put((byte) 0).flip();
        sendMessage(MessageType.SELECT_MINBIAS, buf);
    }

    @Override
    public void enableMinBias() throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.put((byte) 1).flip();
        sendMessage(MessageType.SELECT_MINBIAS, buf);
    }

    @Override
    public FastMoniRateType getFastMoniRateType() throws MessageException
    {
        ByteBuffer buf = sendMessage(MessageType.GET_FAST_MONI_RATE_TYPE);
        byte type = buf.get();
        if (type == 0) return FastMoniRateType.F_MONI_RATE_HLC;
        return FastMoniRateType.F_MONI_RATE_SLC;
    }

    @Override
    public void setFastMoniRateType(FastMoniRateType type) throws MessageException
    {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put((byte) type.ordinal()).flip();
        sendMessage(MessageType.SET_FAST_MONI_RATE_TYPE, buf);
    }

}
