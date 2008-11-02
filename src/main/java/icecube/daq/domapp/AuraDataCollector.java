package icecube.daq.domapp;

import icecube.daq.bindery.BufferConsumer;
import icecube.daq.dor.Driver;
import icecube.daq.dor.GPSException;
import icecube.daq.dor.GPSInfo;
import icecube.daq.dor.TimeCalib;
import icecube.daq.rapcal.RAPCal;
import icecube.daq.rapcal.RAPCalException;
import icecube.daq.rapcal.ZeroCrossingRAPCal;
import icecube.daq.util.UTC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

public class AuraDataCollector extends AbstractDataCollector
{
    private AuraDRM drm;
    private Driver driver;
    private RAPCal rapcal;
    private BufferConsumer hits;
    private AtomicBoolean running;
    private static final Logger logger = Logger.getLogger(AuraDataCollector.class);
    
    public AuraDataCollector(int card, int pair, char dom, BufferConsumer hits)
    {
        super(card, pair, dom);
        driver = Driver.getInstance();
        rapcal = new ZeroCrossingRAPCal();
        new Timer().schedule(new TCALTask(), 1000L, 1000L);
        this.hits = hits;
    }
    
    public void run()
    {
        try
        {
            driver.softboot(card, pair, dom);
            drm = new AuraDRM(card, pair, dom);
            String mbid = drm.getMainboardId();
            long mbid_n = Long.parseLong(mbid, 16);
            setRunLevel(RunLevel.IDLE);
            
            while (running.get() && !interrupted())
            {
                if (isRunning())
                {
                    ByteBuffer buf = drm.forcedTrig(1);
                    buf.order(ByteOrder.LITTLE_ENDIAN);
                    long domclk = buf.getLong(28);
                    long utc = rapcal.domToUTC(domclk).in_0_1ns();
                    ByteBuffer xtb = ByteBuffer.allocate(4886);
                    xtb.putInt(4886);
                    xtb.putInt(641);
                    xtb.putLong(mbid_n);
                    xtb.putLong(domclk);
                    xtb.putLong(utc);
                    xtb.put(buf);
                    xtb.rewind();
                    hits.consume(xtb);
                }
            }
        
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }
    
    @Override
    public void close()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public long getAcquisitionLoopCount()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getNumHits()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getNumMoni()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getNumSupernova()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getNumTcal()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void signalShutdown()
    {
        running.set(false);

    }
    
    private class TCALTask extends TimerTask
    {
        private UTC gpsOffset;
        
        TCALTask()
        {
            gpsOffset = new UTC();
        }
        
        @Override
        public void run()
        {
            // TODO Auto-generated method stub
            try
            {
                try
                {
                    GPSInfo gps = driver.readGPS(card);
                    gpsOffset = gps.getOffset();
                }
                catch (GPSException gpsx)
                {
                    logger.warn("GPS exception");
                }
                TimeCalib tcal = driver.readTCAL(card, pair, dom);
                synchronized (rapcal)
                {
                    rapcal.update(tcal, gpsOffset);
                }
            }
            catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (RAPCalException rcx)
            {
                rcx.printStackTrace();
            }
            
        }
        
    }

}
