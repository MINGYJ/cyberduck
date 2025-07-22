package ch.cyberduck.core.irods;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.irods.irods4j.high_level.connection.IRODSConnectionPool.PoolConnection;
import org.irods.irods4j.high_level.io.IRODSDataObjectInputStream;
import org.irods.irods4j.high_level.io.IRODSDataObjectStream.SeekDirection;
import org.irods.irods4j.low_level.api.IRODSException;

public class ChunkDownload implements Runnable{
	private final IRODSDataObjectInputStream in;
    private final RandomAccessFile out;
    private final long offset;
    private final long chunkSize;
    private final byte[] buffer;
	public ChunkDownload(IRODSDataObjectInputStream stream, String localfilePath,long offset, long chunkSize, int bufferSize) throws IOException, IRODSException {
		this.in= stream;
		this.out=new RandomAccessFile(localfilePath,"rw");
		this.offset=offset;
		this.chunkSize=chunkSize;
		this.buffer=new byte[bufferSize];
		
		in.seek((int)offset, SeekDirection.BEGIN);
		out.seek(offset);
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		//
		long remaining = chunkSize;
		while(remaining>0) {
			try {
				 int readLength = (int) Math.min(buffer.length, remaining);
				int read = this.in.read(buffer,0,readLength);
				out.write(buffer, 0, read);
		        remaining -= read;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
//		try {
//			close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	
	public void close() throws IOException {
		in.close();
		out.close();
	}
}
