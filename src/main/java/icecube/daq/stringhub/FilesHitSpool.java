package icecube.daq.stringhub;

import icecube.daq.bindery.BufferConsumer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
    private BufferConsumer out;
    private ByteBuffer iobuf;
    private int bufferSize = 16384;
    private int  maxNumberOfFiles = 100;
    private int  currentFileIndex = -1;
    private File targetDirectory;
    private FileChannel ch;
    private long t;
    private long t0;
    private long fileInterval;
    
    private final static Logger logger = Logger.getLogger(FilesHitSpool.class);
    
    /**
     * Constructor with full options.
     * @param out   BufferConsumer object that will receive forwarded hits.  Can be null.
     * @param targetDir output directory on filesystem 
     * @param hitsPerFile number of hits per file
     * @param fileCount number of files in the spooling ensemble
     * @see BufferConsumer
     */
    public FilesHitSpool(BufferConsumer out, File targetDir, long fileInterval, int fileCount) throws IOException
    {
        this.out = out;
        this.iobuf = ByteBuffer.allocateDirect(bufferSize);
        this.t0  = 0L;
        this.fileInterval = fileInterval;
        this.targetDirectory = targetDir;
        this.maxNumberOfFiles = fileCount;
        this.ch = null;
    }
    
    public FilesHitSpool(BufferConsumer out, File targetDir, long hitsPerFile) throws IOException
    {
        this(out, targetDir, hitsPerFile, 100);
    }
    
    public FilesHitSpool(BufferConsumer out, File targetDir) throws IOException
    {
        this(out, targetDir, 100000L);
    }
    
    public void consume(ByteBuffer buf) throws IOException
    {
        // bytes 24 .. 31 hold the 64-bit UTC clock value
        t = buf.getLong(24);
        if (t0 == 0L) t0 = t;
        long deltaT = t - t0;
        int fileNo = ((int) (deltaT / fileInterval)) % maxNumberOfFiles; 
        
        if (fileNo != currentFileIndex)
        {
            currentFileIndex = fileNo;
            openNewFile();
        }
        
        int prev_limit = buf.limit();
        
        do
        {
            // transfer buf into iobuf
            int x = iobuf.remaining();
            int n = buf.remaining();
            if (x < n) buf.limit(x); 
            iobuf.put(buf);
            buf.limit(prev_limit);
            if (iobuf.remaining() == 0)
            {
                iobuf.flip();
                while (iobuf.remaining() > 0) ch.write(iobuf);
                iobuf.clear();
            }
        } 
        while (buf.remaining() > 0);
        
        if (null != out) out.consume(buf);
    }

    private void openNewFile() throws IOException
    {
        String fileName = "HitSpool-" + currentFileIndex + ".dat";
        File newFile = new File(targetDirectory, fileName);
        File infFile = new File(targetDirectory, "info.txt");
        
        // flush the current iobuf, if non-empty
        if (ch != null)
        {
            iobuf.flip();
            while (iobuf.remaining() > 0) ch.write(iobuf);
            iobuf.clear();
            ch.close();
        }
        
        FileOutputStream ostr = new FileOutputStream(infFile);
        FileLock lock = ostr.getChannel().lock();
        PrintStream info = new PrintStream(ostr);
        try 
        {
            info.println(String.format("T0   %20d", t0));
            info.println(String.format("CURT %20d", t));
            info.println(String.format("IVAL %20d", fileInterval));
            info.println(String.format("CURF %4d", currentFileIndex));
            info.println(String.format("MAXF %4d", maxNumberOfFiles));
        }
        finally
        {
            lock.release();
            info.close();
        }
        RandomAccessFile raFile = new RandomAccessFile(newFile, "rw");
        raFile.seek(0L);
        ch = raFile.getChannel();
    }
}
