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
import ch.cyberduck.core.PathAttributes;
import ch.cyberduck.core.exception.BackgroundException;
import ch.cyberduck.core.features.Write;
import ch.cyberduck.core.io.StatusOutputStream;
import ch.cyberduck.core.transfer.TransferStatus;
import ch.cyberduck.core.worker.DefaultExceptionMappingService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.OutputStream;

import org.apache.commons.io.FilenameUtils;
import org.irods.irods4j.common.Versioning;
import org.irods.irods4j.high_level.catalog.IRODSQuery;
import org.irods.irods4j.high_level.catalog.IRODSQuery.GenQuery1QueryArgs;
import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.io.IRODSDataObjectOutputStream;
import org.irods.irods4j.high_level.vfs.IRODSFilesystem;
import org.irods.irods4j.low_level.api.GenQuery1Columns;
import org.irods.irods4j.low_level.api.IRODSException;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.exception.JargonRuntimeException;
import org.irods.jargon.core.packinstr.DataObjInp;
import org.irods.jargon.core.pub.IRODSFileSystemAO;
import org.irods.jargon.core.pub.domain.ObjStat;
import org.irods.jargon.core.pub.io.IRODSFileOutputStream;
import org.irods.jargon.core.pub.io.PackingIrodsOutputStream;

public class IRODSWriteFeature implements Write<List<String>> {

    private final IRODSSession session;

    public IRODSWriteFeature(IRODSSession session) {
        this.session = session;
    }

    @Override
    public StatusOutputStream<List<String>> write(final Path file, final TransferStatus status, final ConnectionCallback callback) throws BackgroundException {
        try {
            try {
                final IRODSConnection conn = session.getClient();
//                final IRODSFileOutputStream out = fs.getIRODSFileFactory().instanceIRODSFileOutputStream(
//                        file.getAbsolute(), status.isAppend() ? DataObjInp.OpenFlags.READ_WRITE : DataObjInp.OpenFlags.WRITE_TRUNCATE);
//                return new StatusOutputStream<ObjStat>(new PackingIrodsOutputStream(out)) {
//                    @Override
//                    public ObjStat getStatus() throws BackgroundException {
//                        // No remote attributes from server returned after upload
//                        try {
//                            return fs.getObjStat(file.getAbsolute());
//                        }
//                        catch(JargonException e) {
//                            throw new IRODSExceptionMappingService().map("Failure to read attributes of {0}", e, file);
//                        }
//                    }
//                };
                boolean append = status.isAppend();
                boolean truncate = !append;
                final OutputStream out = new IRODSDataObjectOutputStream(conn.getRcComm(), file.getAbsolute(), truncate, append);
                return new StatusOutputStream<List<String>>(out) {
                    @Override
                    public List<String> getStatus() throws BackgroundException {
                    	String logicalPath = file.getAbsolute();
                    	String parentPath = FilenameUtils.getFullPathNoEndSeparator(logicalPath);
                    	String fileName = FilenameUtils.getName(logicalPath);
                    	final List<String> status = new ArrayList<>();;
                        if(Versioning.compareVersions(conn.getRcComm().relVersion.substring(4), "4.3.4") > 0) {
                    		String query = String.format("select DATA_MODIFY_TIME, DATA_CREATE_TIME, DATA_SIZE, DATA_CHECKSUM, DATA_OWNER_NAME, DATA_OWNER_ZONE where COLL_NAME = '%s' and DATA_NAME = '%s'", parentPath, fileName);
                    		List<List<String>> rows;
							try {
								rows = IRODSQuery.executeGenQuery2(conn.getRcComm(), query);
								status.addAll(rows.get(0));
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (IRODSException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
                    	}else {
                    		var input = new GenQuery1QueryArgs();

                			// select COLL_NAME, DATA_NAME, DATA_ACCESS_TIME
                			input.addColumnToSelectClause(GenQuery1Columns.COL_D_MODIFY_TIME);
                			input.addColumnToSelectClause(GenQuery1Columns.COL_D_CREATE_TIME);
                			input.addColumnToSelectClause(GenQuery1Columns.COL_DATA_SIZE);
                			input.addColumnToSelectClause(GenQuery1Columns.COL_D_DATA_CHECKSUM);
                			input.addColumnToSelectClause(GenQuery1Columns.COL_D_OWNER_NAME);
                			input.addColumnToSelectClause(GenQuery1Columns.COL_D_OWNER_ZONE);
                			

                			// where COLL_NAME like '/tempZone/home/rods and DATA_NAME = 'atime.txt'
                			var collNameCondStr = String.format("= '%s'", parentPath);
                			var dataNameCondStr = String.format("= '%s'", fileName);
                			input.addConditionToWhereClause(GenQuery1Columns.COL_COLL_NAME, collNameCondStr);
                			input.addConditionToWhereClause(GenQuery1Columns.COL_DATA_NAME, dataNameCondStr);

                			var output = new StringBuilder();

                			try {
								IRODSQuery.executeGenQuery1(conn.getRcComm(), input, row -> {
									status.addAll(row);
									return false;
								});
							} catch (IOException | IRODSException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
                    	}
                        return status;
                    }
                };
            }
            catch(IOException | IRODSException e) {
                if(e.getCause() instanceof JargonException) {
                    throw (JargonException) e.getCause();
                }
                throw new DefaultExceptionMappingService().map(e);
            }
        }
        catch(JargonException e) {
            throw new IRODSExceptionMappingService().map("Uploading {0} failed", e, file);
        }
    }
}
