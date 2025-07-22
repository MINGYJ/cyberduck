package ch.cyberduck.core.irods;

/*
 * Copyright (c) 2002-2015 David Kocher. All rights reserved.
 * http://cyberduck.ch/
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * Bug fixes, suggestions and comments should be sent to feedback@cyberduck.ch
 */

import ch.cyberduck.core.ConnectionCallback;
import ch.cyberduck.core.Host;
import ch.cyberduck.core.Local;
import ch.cyberduck.core.Path;
import ch.cyberduck.core.exception.BackgroundException;
import ch.cyberduck.core.exception.NotfoundException;
import ch.cyberduck.core.features.Download;
import ch.cyberduck.core.features.Read;
import ch.cyberduck.core.io.BandwidthThrottle;
import ch.cyberduck.core.io.StreamListener;
import ch.cyberduck.core.preferences.HostPreferencesFactory;
import ch.cyberduck.core.preferences.PreferencesFactory;
import ch.cyberduck.core.transfer.TransferStatus;

import org.apache.commons.lang3.StringUtils;
import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.IRODSConnectionPool;
import org.irods.irods4j.high_level.connection.IRODSConnectionPool.PoolConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.high_level.io.IRODSDataObjectInputStream;
import org.irods.irods4j.high_level.io.IRODSDataObjectOutputStream;
import org.irods.irods4j.high_level.io.IRODSDataObjectStream.SeekDirection;
import org.irods.irods4j.high_level.vfs.IRODSFilesystem;
import org.irods.irods4j.low_level.api.IRODSApi;
import org.irods.irods4j.low_level.api.IRODSApi.RcComm;
import org.irods.irods4j.low_level.api.IRODSException;
//import org.irods.jargon.core.exception.JargonException;
//import org.irods.jargon.core.packinstr.TransferOptions;
//import org.irods.jargon.core.pub.DataTransferOperations;
//import org.irods.jargon.core.pub.IRODSFileSystemAO;
//import org.irods.jargon.core.pub.io.IRODSFile;
//import org.irods.jargon.core.transfer.DefaultTransferControlBlock;
//import org.irods.jargon.core.transfer.TransferControlBlock;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.FileOutputStream;

public class IRODSDownloadFeature implements Download {

    private final IRODSSession session;
    private boolean truncate = true;
	private boolean append = false;
	private static final int BUFFER_SIZE = 4 * 1024 * 1024; 

    public IRODSDownloadFeature(final IRODSSession session) {
        this.session = session;
    }

    @Override
    public void download(final Path file, final Local local, final BandwidthThrottle throttle,
                         final StreamListener listener, final TransferStatus status,
                         final ConnectionCallback callback) throws BackgroundException {
    	final int numThread = 3;
        try {
//            final IRODSFileSystemAO fs = session.getClient();
//            final IRODSFile f = fs.getIRODSFileFactory().instanceIRODSFile(file.getAbsolute());
//            if(f.exists()) {
//                final TransferControlBlock block = DefaultTransferControlBlock.instance(StringUtils.EMPTY,
//                        HostPreferencesFactory.get(session.getHost()).getInteger("connection.retry"));
//                final TransferOptions options = new DefaultTransferOptionsConfigurer().configure(new TransferOptions());
//                if(Host.TransferType.unknown.equals(session.getHost().getTransferType())) {
//                    options.setUseParallelTransfer(Host.TransferType.valueOf(PreferencesFactory.get().getProperty("queue.transfer.type")).equals(Host.TransferType.concurrent));
//                }
//                else {
//                    options.setUseParallelTransfer(session.getHost().getTransferType().equals(Host.TransferType.concurrent));
//                }
//                block.setTransferOptions(options);
//                final DataTransferOperations transfer = fs.getIRODSAccessObjectFactory()
//                    .getDataTransferOperations(fs.getIRODSAccount());
//                transfer.getOperation(f, new File(local.getAbsolute()),
//                    new DefaultTransferStatusCallbackListener(status, listener, block),
//                    block);
//            }
//            else {
//                throw new NotfoundException(file.getAbsolute());
//            }
        	
//NonParallel transfer
//        	 final RcComm conn = session.getClient().getRcComm();
//             final String logicalPath = file.getAbsolute();
//             if (!IRODSFilesystem.exists(conn, logicalPath)) {
//                 throw new NotfoundException(logicalPath);
//             }
//             try (InputStream in = new IRODSDataObjectInputStream(conn, file.getAbsolute());
//            	     OutputStream out = new FileOutputStream(local.getAbsolute())) {
//
//            	    in.transferTo(out);
//            	}
        	 final RcComm primaryConn = session.getClient().getRcComm();
             final String logicalPath = file.getAbsolute();

             if (!IRODSFilesystem.exists(primaryConn, logicalPath)) {
                 throw new NotfoundException(logicalPath);
             }

             final long fileSize = IRODSFilesystem.dataObjectSize(primaryConn, logicalPath);

             // Step 1: Get replica token & number via primary stream
             try (IRODSDataObjectInputStream primary = new IRODSDataObjectInputStream(primaryConn, logicalPath)) {
                 final String replicaToken = primary.getReplicaToken();
                 final long replicaNumber = primary.getReplicaNumber();

                 // Step 2: Setup connection pool
                 final IRODSConnectionPool pool = new IRODSConnectionPool(numThread);
                 pool.start(
                     session.getHost().getHostname(),
                     session.getHost().getPort(),
                     new QualifiedUsername(session.getHost().getCredentials().getUsername(), session.getRegion()),
                     conn -> {
                         try {
                             IRODSApi.rcAuthenticateClient(conn, "native", session.getHost().getCredentials().getPassword());
                             return true;
                         } catch (Exception e) {
                             return false;
                         }
                     });

                 final ExecutorService executor = Executors.newFixedThreadPool(numThread);
                 
                 //TODO:fileSize/
                 final long chunkSize = fileSize / numThread;
                 final long remainChunkSize = fileSize % numThread;


                 // Step 3: Create empty target file
                 try (RandomAccessFile out = new RandomAccessFile(local.getAbsolute(), "rw")) {
                     out.setLength(fileSize);
                 }

                 // Step 4: Parallel readers
                 List<Future<?>> tasks = new ArrayList<>();
                 for (int i = 0; i < numThread; i++) {
//                     final int threadId = i;
//                     tasks.add(executor.submit(() -> {
//                         final long start = threadId * chunkSize;
//                         final long end = Math.min(fileSize, start + chunkSize);
//                         final int length = (int) (end - start);
//
//                         try (
//                        	 PoolConnection conn = pool.getConnection();
//                        	IRODSDataObjectInputStream stream = new IRODSDataObjectInputStream(conn.getRcComm(), replicaToken, replicaNumber)
//                         ) {
//                             stream.seek((int) start,SeekDirection.CURRENT);
//                             //TODO:fixed chunk size
//                             byte[] buffer = new byte[length];
//                             int read = stream.read(buffer);
//                             //TODO: always open the file
//                             try (RandomAccessFile out = new RandomAccessFile(local.getAbsolute(), "rw")) {
//                                 out.seek(start);
//                                 out.write(buffer, 0, read);	
//                             }
//                         }
//                         return null;
//                     }));
                     final long start = i * chunkSize;
                     final PoolConnection conn = pool.getConnection();
                     IRODSDataObjectInputStream stream = new IRODSDataObjectInputStream(conn.getRcComm(), replicaToken, replicaNumber);
                     //tasks.add(executor.submit(() -> {}));
                     ChunkDownload worker = new ChunkDownload(
                    	        stream,
                    	        local.getAbsolute(),
                    	        start,
                    	        (numThread - 1 == i) ? chunkSize + remainChunkSize : chunkSize,
                    	        BUFFER_SIZE
                    	    );
                     Future<?> task = executor.submit(worker);
                     tasks.add(task);
                 }

                 for (Future<?> task : tasks) {
                     task.get();
                 }


                 executor.shutdown();
                 pool.close();
             }
             
        }
        catch(Exception e) {
            throw new IRODSExceptionMappingService().map("Download {0} failed", e);
        }
    }

    @Override
    public boolean offset(final Path file) {
        return false;
    }

    @Override
    public Download withReader(final Read reader) {
        return this;
    }
}
