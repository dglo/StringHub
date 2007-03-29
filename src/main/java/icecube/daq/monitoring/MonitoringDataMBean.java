package icecube.daq.monitoring;

/**
 * MBean interface for StringHub back-end data.
 */
public interface MonitoringDataMBean
{
    /**
     * Get average number of hits per readout.
     *
     * @return average hits per readout
     */
    //long getAverageHitsPerReadout();

    /**
     * Get back-end timing profile.
     *
     * @return back end timing
     */
    //String getBackEndTiming();

    /**
     * Get current rate of hits per second.
     *
     * @return hits per second
     */
    //double getHitsPerSecond();

    /**
     * Get the time of the most recently queued hit.
     *
     * @return latest time
     */
    //long getLatestHitTime();

    /**
     * Get the end time of the most recent readout data payload.
     *
     * @return latest readout data end time
     */
    //long[] getLatestReadoutTimes();

    /**
     * Get number of hits which could not be loaded.
     *
     * @return num bad hits
     */
    //long getNumBadHits();

    /**
     * Number of readout requests which could not be loaded.
     *
     * @return num bad readout requests
     */
    //long getNumBadReadoutRequests();

    /**
     * Get number of passes through the main loop without a request.
     *
     * @return num empty loops
     */
    //long getNumEmptyLoops();

    /**
     * Get number of hits cached for readout being built
     *
     * @return num hits cached
     */
    int getNumHitsCached();

    /**
     * Get number of hits thrown away.
     *
     * @return num hits discarded
     */
    //long getNumHitsDiscarded();

    /**
     * Get number of hits queued for processing.
     *
     * @return num hits queued
     */
    int getNumHitsQueued();

    /**
     * Get number of hits received.
     *
     * @return num hits received
     */
    long getNumHitsReceived();

    /**
     * Get number of null hits received.
     *
     * @return num null hits
     */
    //long getNumNullHits();

    /**
     * Get number of readouts which could not be created.
     *
     * @return num null readouts
     */
    //long getNumNullReadouts();

    /**
     * Number of readout requests dropped while stopping.
     *
     * @return num readout requests dropped
     */
    //long getNumReadoutRequestsDropped();

    /**
     * Number of readout requests currectly queued for processing.
     *
     * @return num readout requests queued
     */
    long getNumReadoutRequestsQueued();

    /**
     * Number of readout requests received for this run.
     *
     * @return num readout requests received
     */
    long getNumReadoutRequestsReceived();

    /**
     * Get number of readouts which could not be sent.
     *
     * @return num readouts failed
     */
    //long getNumReadoutsFailed();

    /**
     * Get number of empty readouts which were ignored.
     *
     * @return num readouts ignored
     */
    //long getNumReadoutsIgnored();

    /**
     * Get number of readouts sent.
     *
     * @return num readouts sent
     */
    //long getNumReadoutsSent();

    /**
     * Get number of recycled payloads.
     *
     * @return num recycled
     */
    //long getNumRecycled();

    /**
     * Get number of hits not used for a readout.
     *
     * @return num unused hits
     */
    //long getNumUnusedHits();
}
