package icecube.daq.stringhub;

import icecube.daq.bindery.BufferConsumer;
import icecube.daq.util.DOMRegistry;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;

import org.apache.log4j.Logger;

/**
 * This class will accept ByteBuffers from whatever is feeding it
 * and spool the byte buffer contents (assumed to be hits but
 * really it can be anything) to a rotating list of files on
 * the filesystem.  Each file will have a certain number of buffers
 * written to it after which the file will be closed and the next
 * one opened up with a sequential identifier indicating that it is
 * the next file.  Once the maximum number of files is achieved,
 * the sequence will start from the beginning, overwriting files.
 *
 * @author kael hanson (khanson@ulb.ac.be)
 *
 */
public class FilesHitSpool implements BufferConsumer
{
    public static final String PACK_HEADERS_PROPERTY =
        "icecube.daq.stringhub.hitspool.packHeaders";

    private BufferConsumer out;
    private int  maxNumberOfFiles;
    private int  currentFileIndex;
    private File hitSpoolDir;
    private File targetDirectory;
    private OutputStream dataOut;
    private long t;
    private long t0;
    private long fileInterval;
    private DOMRegistry registry;
    private boolean packHeaders;
    private byte[] iobuf;
    private boolean isHosed;

    private static final Logger logger = Logger.getLogger(FilesHitSpool.class);

    /**
     * Constructor with full options.
     * @param out BufferConsumer object that will receive forwarded hits.
     *            Can be null.
     * @param configDir directory holding configuration files
     * @param targetDir top-level directory which holds hitspool directories
     * @param fileInterval number of DAQ ticks of objects in each file
     * @param fileCount number of files in the spooling ensemble
     * @see BufferConsumer
     */
    public FilesHitSpool(BufferConsumer out, File configDir, File targetDir,
                         long fileInterval, int fileCount)
        throws IOException
    {
        this.out          = out;
        this.fileInterval = fileInterval;
        hitSpoolDir       = targetDir;
        maxNumberOfFiles  = fileCount;
        packHeaders       = Boolean.getBoolean(PACK_HEADERS_PROPERTY);

        if (packHeaders && configDir != null) {
            try {
                registry = DOMRegistry.loadRegistry(configDir);
            } catch (Exception x) {
                registry = null;
            }
        }

        logger.info("DOM registry " + (registry != null? "" : "NOT") +
                    " found; header packing " + (packHeaders ? "" : "NOT") +
                    " activated.");

        iobuf = new byte[5000];

        // make sure hitspool directory exists
        if (hitSpoolDir == null) {
            throw new IOException("Hit spool directory cannot be null");
        } else if (!hitSpoolDir.exists()) {
            hitSpoolDir.mkdirs();
        }
    }

    public FilesHitSpool(BufferConsumer out, File configDir, File targetDir,
                         long hitsPerFile)
        throws IOException
    {
        this(out, configDir, targetDir, hitsPerFile, 100);
    }

    public FilesHitSpool(BufferConsumer out, File configDir, File targetDir)
        throws IOException
    {
        this(out, configDir, targetDir, 100000L);
    }

    private int transform(ByteBuffer buf)
    {
        int cpos = 0;
        // Here's your chance to compactify the buffer
        if (registry != null && packHeaders) {
            buf.putShort(0, (short)(0xC000 | (buf.getInt(0)-24)));
            long mbid = buf.getLong(8);
            short chid = registry.getChannelId(mbid);
            buf.putShort(2, chid);
            buf.putLong(4, buf.getLong(24));
            // pack in the version (bytes 34-35) and
            // the PED / ATWD-chargestamp flags (36-37)
            buf.putShort(12, (short)((buf.getShort(34) & 0xff) |
                                     ((buf.getShort(36) & 0xff) << 8)));
            buf.get(iobuf, 0, 14);
            buf.position(38);
            cpos = 14;
        }
        int br = buf.remaining();
        buf.get(iobuf, cpos, br);
        buf.rewind();
        return br + cpos;
    }

    public void consume(ByteBuffer buf) throws IOException
    {
        if (null != out) {
            // pass buffer to other consumers first
            out.consume(buf);
        }

        if (isHosed) return;
        if (buf.limit() < 38) {
            if (buf.limit() != 0) {
                logger.error("Skipping short buffer (" + buf.limit() +
                             " bytes)");
            }
            return;
        }

        // bytes 24 .. 31 hold the 64-bit UTC clock value
        t = buf.getLong(24);

        if (t == Long.MAX_VALUE) {
            // this is the END-OF-DATA marker
            handleEndOfStream();
            return;
        }

        // remember the first time
        if (t0 == 0L) t0 = t;

        // make sure we write to the appropriate file
        int fileNo = ((int) ((t - t0) / fileInterval)) % maxNumberOfFiles;
        if (fileNo != currentFileIndex) {
            currentFileIndex = fileNo;
            try {
                openNewFile();
            } catch (IOException iox) {
                logger.error("openNewFile threw " + iox.getMessage() +
                             ". HitSpooling will be terminated.");
                dataOut = null;
                isHosed = true;
                return;
            }
        }

        // now I should be free to pack the buffer, if that is the
        // behavior desired.
        int nw = transform(buf);

        try {
            dataOut.write(iobuf, 0, nw);
        } catch (IOException iox) {
            logger.error("hit spool writing failed b/c " + iox.getMessage() +
                         ". Hit spooling will be terminated.");
            dataOut = null;
            isHosed = true;
        }
    }

    /**
     * There will be no more data.
     */
    public void endOfStream(long mbid)
        throws IOException
    {
        if (null != out) out.endOfStream(mbid);
        handleEndOfStream();
    }

    /**
     * Close output file and quit
     * NOTE: there could be multiple End-Of-Streams
     *
     * @throws IOException if the output stream could not be closed
     */
    private void handleEndOfStream()
        throws IOException
    {
        if (dataOut != null) {
            dataOut.close();
            dataOut = null;
        }
    }

    /**
     * Create a new hitspool data file and update the associated metadata file
     *
     * @throws IOException if there is a problem
     */
    private synchronized void openNewFile()
        throws IOException
    {
        // close current file
        if (dataOut != null) {
            try {
                dataOut.flush();
                dataOut.close();
            } catch (IOException ioe) {
                logger.error("Failed to close previous hitspool file", ioe);
            }
        }

        // update metadata file
        File infFile = new File(targetDirectory, "info.txt");
        FileOutputStream ostr = new FileOutputStream(infFile);
        FileLock lock = ostr.getChannel().lock();
        PrintStream info = new PrintStream(ostr);
        try {
            info.println(String.format("T0   %20d", t0));
            info.println(String.format("CURT %20d", t));
            info.println(String.format("IVAL %20d", fileInterval));
            info.println(String.format("CURF %4d", currentFileIndex));
            info.println(String.format("MAXF %4d", maxNumberOfFiles));
        } finally {
            lock.release();
            info.close();
        }

        // open new file
        final String fileName = "HitSpool-" + currentFileIndex + ".dat";
        File newFile = new File(targetDirectory, fileName);
        dataOut =
            new BufferedOutputStream(new FileOutputStream(newFile), 32768);
    }

    /**
     * Reset internal counters
     */
    private void reset()
        throws IOException
    {
        rotateDirectories();

        t0  = 0L;
        currentFileIndex   = -1;
    }

    /**
     * Swap current and last directories.
     *
     * synchronized to avoid clashes with openNewFile()
     *
     * @throws IOException if a file/directory cannot be created
     */
    private synchronized void rotateDirectories()
        throws IOException
    {
        // create and delete a temporary file in the hitspool directory
        File hitSpoolTemp = File.createTempFile("HitSpool", ".tmp",
                                                hitSpoolDir);
        if (!hitSpoolTemp.delete()) {
            throw new IOException("Could not delete temporary file " +
                                  hitSpoolTemp);
        }

        // create and remove a temporary directory
        if (!hitSpoolTemp.exists()) {
            if (!hitSpoolTemp.mkdirs()) {
                throw new IOException("Could not create temporary directory " +
                                      hitSpoolTemp);
            }
        }
        if (!hitSpoolTemp.delete()) {
            throw new IOException("Could not delete temporary directory " +
                                  hitSpoolTemp);
        }

        // Note that renameTo and mkdir return false on failure

        // Rename lastRun to temporary directory name
        File hitSpoolLast = new File(hitSpoolDir, "lastRun");
        if (hitSpoolLast.exists() && !hitSpoolLast.renameTo(hitSpoolTemp)) {
            logger.error("hitSpoolLast renameTo failed");
        }

        // Rename currentRun to lastRun
        File hitSpoolCurrent = new File(hitSpoolDir, "currentRun");
        if (hitSpoolCurrent.exists() &&
            !hitSpoolCurrent.renameTo(hitSpoolLast))
        {
            logger.error("hitSpoolCurrent renameTo failed!");
        }

        // Rename lastRun to currentRun
        if (hitSpoolTemp.exists() && !hitSpoolTemp.renameTo(hitSpoolCurrent)) {
            logger.debug("hitSpoolTemp renameTo failed!");
        }

        // if currentRun doesn't exist yet, create it now
        if (!hitSpoolCurrent.exists() && !hitSpoolCurrent.mkdir()) {
            logger.error("hitSpoolCurrent mkdir failed!");
        }

        // finally set target directory to currentRun
        targetDirectory = hitSpoolCurrent;
    }

    /**
     * Start a new run
     *
     * @param runNumber run number
     *
     * @throws IOException if hitspool directories cannot be created
     */
    public void startRun(int runNumber)
        throws IOException
    {
        reset();
    }

    /**
     * Switch to a new run
     *
     * @param runNumber run number
     *
     * @throws IOException if hitspool directories cannot be created
     */
    public void switchRun(int runNumber)
        throws IOException
    {
        reset();
    }
}
