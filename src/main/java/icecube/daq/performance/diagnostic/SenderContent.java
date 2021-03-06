package icecube.daq.performance.diagnostic;

import icecube.daq.monitoring.SenderMXBean;

/**
 * Provides trace content derived from a Sender instance.
 */
public class SenderContent implements Content
{
    private final SenderMXBean sender;
    private final String header;

    public SenderContent(final SenderMXBean sender)
    {
        this.sender = sender;
        this.header = String.format("%-10s %-10s %-10s %-10s", "hitsq",
                "readq", "readsent", "lat(s)");

    }

    @Override
    public void header(final StringBuilder sb)
    {
        sb.append(header);
    }

    @Override
    public void content(final StringBuilder sb)
    {
        final int hitsQueued = sender.getNumHitsQueued();
        final long readoutsQueued = sender.getNumReadoutRequestsQueued();
        final long readouts = sender.getNumReadoutsSent();
        final double latency = sender.getReadoutLatency()/10000000000d;
        sb.append(String.format("%-10d %-10d %-10d %-10.1f", hitsQueued,
                readoutsQueued, readouts, latency));
    }

}
