package icecube.daq.domapp;

public class LocalCoincidenceConfiguration
{
    /**
     * Enumeration of constants which control the behavior of 
     * ATWD and FADC waveform transmission depending on state
     * of LC signals from upper / lower neighbors
     * @author kael
     */
    public enum RxMode
    {
        /**
         * No LC required to send WF
         */
        RXNONE,
        /**
         * Either UP or DOWN LC signal will cause WF x-mit
         */
        RXEITHER, 
        /**
         * Only state of UP LC signal matters
         */
        RXUP, 
        /**
         * Only state of DOWN LC signal matters
         */
        RXDOWN, 
        /**
         * Need both UP.AND.DOWN LC signals simultaneously
         */
        RXBOTH, 
        /**
         * Only send SLC header packets no matter what.
         */
        RXHDRS;
        public byte asByte() 
        { 
            return (byte) ordinal(); 
        }
    }

    public enum Source
    {
        SPE, MPE;
        public byte asByte() 
        { 
            return (byte) ordinal(); 
        }
    }

    public enum TxMode
    {
        TXNONE, TXDOWN, TXUP, TXBOTH;
        public byte asByte() 
        { 
            return (byte) ordinal(); 
        }
    }

    /**
     * Enum class to handle LC types
     * @author krokodil
     *
     */
    public enum Type
    {
        SOFT(1), HARD(2), FLABBY(3);
        private byte value;
        Type(int val) 
        {
            this.value = (byte) val; 
        }
        public byte asByte() 
        { 
            return this.value; 
        }
    }

    private Type    type;
    private RxMode    rxMode;
    private TxMode    txMode;
    private Source    source;
    private int    preTrigger;
    private int    postTrigger;
    private short[]    cableLengthUp;
    private short[]    cableLengthDn;
    private byte    span;

    public LocalCoincidenceConfiguration()
    {
        type         = Type.HARD;
        rxMode         = RxMode.RXNONE;
        txMode         = TxMode.TXBOTH;
        source        = Source.SPE;
        preTrigger    = 1000;
        postTrigger    = 1000;
        cableLengthUp = new short[] {1000, 1000, 1000, 1000 };
        cableLengthDn = new short[] {1000, 1000, 1000, 1000 };
        span = 1;
    }

    public short[] getCableLengthDn()
    {
        return cableLengthDn;
    }

    public short[] getCableLengthUp() 
    {
        return cableLengthUp;
    }

    public int getPostTrigger()
    {
        return postTrigger;
    }

    public int getPreTrigger()
    {
        return preTrigger;
    }

    /**
     * Returns LC mode as DOMApp byte.
     * @return LC mode byte
     */
    public RxMode getRxMode() 
    {
        return rxMode; 
    }

    public Source getSource()
    {
        return source;
    }

    public byte getSpan() 
    {
        return span;
    }

    /**
     * Returns LC Tx setting as DOMApp byte.
     * @return byte repr of LC Tx setting
     */
    public TxMode getTxMode()
    {
        return txMode;
    }

    /**
     * Returns the LC type in byte format compatible with DOMApp message 
     * SET_LC_TYPE
     * @return DOMApp code for LC type setting
     */
    public Type getType() 
    {
        return type; 
    }

    public void setCableLengthDn(int dist, short delay) 
    {
        this.cableLengthDn[dist] = delay;
    }

    public void setCableLengthUp(int dist, short delay) 
    {
        this.cableLengthUp[dist] = delay;
    }

    public void setPostTrigger(int postTrigger) 
    {
        this.postTrigger = postTrigger;
    }

    public void setPreTrigger(int preTrigger) 
    {
        this.preTrigger = preTrigger;
    }

    public void setRxMode(RxMode mode) 
    { 
        rxMode = mode; 
    }

    public void setSpan(byte span) 
    {
        this.span = span;
    }
    
    public void setSource(Source source)
    {
        this.source = source;
    }

    /**
     * Sets the local coincidence transmit mode.
     * @param mode the LC transmit mode
     */
    public void setTxMode(TxMode mode) 
    { 
        txMode = mode;
    }

    public void setType(Type type) 
    { 
        this.type = type; 
    }
}
