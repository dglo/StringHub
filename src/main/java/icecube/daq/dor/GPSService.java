package icecube.daq.dor;

import icecube.daq.livemoni.LiveTCalMoni;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

/**
 * This class exists to centralize the GPS information collection
 * and hand it out in a more orderly manner to the clients that
 * need it - mostly a gang of DataCollectors who want the GPS
 * offset information.  The GPS procfile only exists on the card
 * level so there must be a one-to-many handoff of this offset info.
 * @author kael
 *
 */

public class GPSService
{

    private Logger logger = Logger.getLogger(GPSService.class);

    private class GPSCollector extends Thread
    {
        private Driver driver;
        //private int card;
        private int cons_gpsx_count;
        private GPSInfo gps;
        private AtomicBoolean running;
        private File gpsFile;

        /** support clients waiting for an available reading. */
        private CountDownLatch initializationLatch = new CountDownLatch(1);

        GPSCollector(Driver driver, int card)
        {
            this.driver = driver;
            //this.card = card;
            this.gpsFile = driver.getGPSFile(card);
            cons_gpsx_count = 0;
            gps = null;
            running = new AtomicBoolean(false);
        }

        void startup()
        {
            running.set(true);
            this.start();
        }

        void shutdown()
        {
            running.set(false);
        }

        public void run()
        {

            try
            {
                // Eat through the up to 10 buffered GPS snaps in the DOR
                for (int i = 0; i < 10; i++)
                    driver.readGPS(gpsFile);
            }
            catch (GPSNotReady nr)
            {
                // OK
            }
            catch (GPSException gpsx)
            {
                // Probably not OK
                gpsx.printStackTrace();
                logger.warn("Got GPS exception " + gpsx.getMessage());
            }
            while (running.get())
            {
                try
                {
                    Thread.sleep(740L);
                    GPSInfo newGPS = driver.readGPS(gpsFile);

                    cons_gpsx_count = 0;
                    if (!(gps == null || newGPS.getOffset().equals(gps.getOffset())))
                    {
                        final String errmsg =
                            "GPS offset mis-alignment detected - old GPS: " +
                            gps + " new GPS: " + newGPS;
                        logger.error(errmsg);
                        if (moni != null) {
                            moni.send(errmsg, null);
                        }
                    }
                    else
                    {
                        synchronized (this)
                        {
                            gps = newGPS;
                            initializationLatch.countDown();
                        }
                    }
                }
                catch (InterruptedException intx)
                {
                    return;
                }
                catch (GPSNotReady gps_not_ready)
                {
                    if (cons_gpsx_count++ > 10)
                    {
                        logger.warn("GPS not ready.");
                        if (moni != null) {
                            moni.send("SyncGPS procfile not ready", null);
                        }
                    }
                }
                catch (GPSException gps_ex)
                {
                    gps_ex.printStackTrace();
                    logger.warn("Got GPS exception - time translation to UTC will be incomplete");
                    if (moni != null) {
                        moni.send(gps_ex.getMessage(), null);
                    }
                }
            }
        }

        synchronized GPSInfo getGps() { return gps; }

        synchronized boolean waitForReady(long waitMillis)
                throws InterruptedException
        {
            return initializationLatch.await(waitMillis, TimeUnit.MILLISECONDS);
        }

        public boolean isRunning()
        {
            return running.get();
        }
    }

    private LiveTCalMoni moni;
    private GPSCollector[] coll;
    private static final GPSService instance = new GPSService();

    private GPSService()
    {
        coll = new GPSCollector[8];
    }

    public static GPSService getInstance() { return instance; }

    public GPSInfo getGps(int card) { return coll[card].getGps(); }

    /**
     * Wait for a valid gps info reading ready to be available.
     *
     * @param waitMillis Time period to wait.
     * @return True if a valid gps infor reading is available.
     * @throws InterruptedException
     */
    public boolean waitForReady(int card, int waitMillis)
            throws InterruptedException
    {
        return coll[card].waitForReady(waitMillis);
    }

    public void startService(Driver drv, int card, LiveTCalMoni moni)
    {
        this.moni = moni;

        if (coll[card] == null) { coll[card] = new GPSCollector(drv, card); }
        if (!coll[card].isRunning()) coll[card].startup();
    }

    public void startService(int card, LiveTCalMoni moni)
    {
        startService(Driver.getInstance(), card, moni);
    }

    public void shutdownAll()
    {
        for (int i = 0; i < 8; i++)
            if (coll[i] != null && coll[i].isRunning()) coll[i].shutdown();
    }
}
