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
import ch.cyberduck.core.Path;
import ch.cyberduck.core.exception.BackgroundException;
import ch.cyberduck.core.features.Copy;
import ch.cyberduck.core.io.StreamListener;
import ch.cyberduck.core.preferences.HostPreferencesFactory;

import org.apache.commons.lang3.StringUtils;
import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.vfs.IRODSCollectionIterator;
import org.irods.irods4j.high_level.vfs.IRODSFilesystem;
import org.irods.irods4j.high_level.vfs.ObjectStatus;
import org.irods.irods4j.low_level.api.IRODSApi.RcComm;
import org.irods.irods4j.low_level.api.IRODSException;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.DataTransferOperations;
import org.irods.jargon.core.pub.IRODSFileSystemAO;
import org.irods.jargon.core.transfer.DefaultTransferControlBlock;
import org.irods.jargon.core.transfer.TransferStatus;
import org.irods.jargon.core.transfer.TransferStatusCallbackListener;

import java.io.IOException;
import java.util.EnumSet;

public class IRODSCopyFeature implements Copy {

    private final IRODSSession session;

    public IRODSCopyFeature(final IRODSSession session) {
        this.session = session;
    }

    @Override
    public Path copy(final Path source, final Path target, final ch.cyberduck.core.transfer.TransferStatus status, final ConnectionCallback callback, final StreamListener listener) throws BackgroundException {
        try {
            final IRODSConnection conn = session.getClient();
            final String from = source.getAbsolute();
            final String to = target.getAbsolute();
//            final DataTransferOperations transfer = fs.getIRODSAccessObjectFactory()
//                .getDataTransferOperations(fs.getIRODSAccount());
//            transfer.copy(fs.getIRODSFileFactory().instanceIRODSFile(source.getAbsolute()),
//                fs.getIRODSFileFactory().instanceIRODSFile(target.getAbsolute()), new TransferStatusCallbackListener() {
//                    @Override
//                    public FileStatusCallbackResponse statusCallback(final TransferStatus transferStatus) {
//                        return FileStatusCallbackResponse.CONTINUE;
//                    }
//
//                    @Override
//                    public void overallStatusCallback(final TransferStatus transferStatus) {
//                        switch(transferStatus.getTransferState()) {
//                            case OVERALL_COMPLETION:
//                                listener.sent(status.getLength());
//                        }
//                    }
//
//                    @Override
//                    public CallbackResponse transferAsksWhetherToForceOperation(final String irodsAbsolutePath, final boolean isCollection) {
//                        return CallbackResponse.YES_THIS_FILE;
//                    }
//                }, DefaultTransferControlBlock.instance(StringUtils.EMPTY,
//                            HostPreferencesFactory.get(session.getHost()).getInteger("connection.retry")));
            if (source.isFile()) {
                IRODSFilesystem.copyDataObject(conn.getRcComm(), from, to);

                if (listener != null && status.getLength() > 0) {
                    listener.sent(status.getLength());
                }
            }
            if(source.isDirectory()) {
            	this.copyDirectoryRecursively(conn.getRcComm(), from, to);
            }
            return target;
        }
        catch(IOException | IRODSException e) {
            throw new IRODSExceptionMappingService().map("Cannot copy {0}", e, source);
        }
    }
    
    public static void copyDirectoryRecursively(RcComm rcComm, String source, String target) throws IOException, IRODSException {
        // First, create the root of the target directory
        if (!IRODSFilesystem.exists(rcComm, target)) {
            IRODSFilesystem.createCollection(rcComm, target);
        }

        // Recursively iterate through the source collection
        for (var entry : new IRODSCollectionIterator(rcComm, source)) {
            String relative = entry.path().substring(source.length()); // relative path from source
            String targetPath = target + relative;

            ObjectStatus status = entry.status();

            if (status.getType() == ObjectStatus.ObjectType.COLLECTION) {
                // Create directory in target
                IRODSFilesystem.createCollection(rcComm, targetPath);
            } else if (status.getType() == ObjectStatus.ObjectType.DATA_OBJECT) {
                // Copy file
                IRODSFilesystem.copyDataObject(rcComm, entry.path(), targetPath);
            }
        }
    }
        
    @Override
    public EnumSet<Flags> features(final Path source, final Path target) {
        return EnumSet.of(Flags.recursive);
    }
}
