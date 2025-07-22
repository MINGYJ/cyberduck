package ch.cyberduck.core.irods;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.irods.irods4j.high_level.io.IRODSDataObjectOutputStream;
import org.irods.irods4j.high_level.io.IRODSDataObjectStream.SeekDirection;
import org.irods.irods4j.low_level.api.IRODSException;

public class ChunkUpload implements Runnable {
    private final IRODSDataObjectOutputStream out;
    private final RandomAccessFile in;
    private final long offset;
    private final long chunkSize;
    private final byte[] buffer;

    public ChunkUpload(IRODSDataObjectOutputStream stream, String localFilePath, long offset, long chunkSize, int bufferSize) throws IOException, IRODSException {
        this.out = stream;
        this.in = new RandomAccessFile(localFilePath, "r");
        this.offset = offset;
        this.chunkSize = chunkSize;
        this.buffer = new byte[bufferSize];

        in.seek(offset);
        out.seek((int) offset, SeekDirection.CURRENT);
    }

    @Override
    public void run() {
        long remaining = chunkSize;
        while (remaining > 0) {
            try {
                int readLength = (int) Math.min(buffer.length, remaining);
                int read = in.read(buffer, 0, readLength);
                if (read == -1) break;
                out.write(buffer, 0, read);
                remaining -= read;
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
    }

    public void close() throws IOException {
        in.close();
        out.close();  // Call this on the final writer only
    }
}
