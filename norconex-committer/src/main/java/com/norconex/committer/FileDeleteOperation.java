/* Copyright 2010-2014 Norconex Inc.
 * 
 * This file is part of Norconex Committer.
 * 
 * Norconex Committer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * Norconex Committer is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Norconex Committer. If not, see <http://www.gnu.org/licenses/>.
 */
package com.norconex.committer;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.commons.lang.file.FileUtil;

/**
 * A file-based deletion operation.
 * @author Pascal Essiembre
 * @since 1.1.0
 */
public class FileDeleteOperation implements IDeleteOperation {

    private static final long serialVersionUID = 1182738593255366952L;
    private static final Logger LOG = 
            LogManager.getLogger(FileDeleteOperation.class);
    
    private final String reference;
    private final File refFile;
    
    /**
     * Constructor.
     * @param refFile the file to be deleted
     */
    public FileDeleteOperation(File refFile) {
        super();
        this.refFile = refFile;
        try {
            this.reference = FileUtils.readFileToString(refFile);
        } catch (IOException e) {
            throw new CommitterException(
                    "Cannot obtain reference from file " + refFile, e);
        }
    }
    
    @Override
    public String getReference() {
        return reference;
    }

    @Override
    public void delete() {
        try {
            FileUtil.delete(refFile);
        } catch (IOException e) {
            LOG.error("Could not delete commit file: " + refFile, e);
        }
    }

    @Override
    public int hashCode() {
        return refFile.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof FileDeleteOperation)) {
            return false;
        }
        FileDeleteOperation other = (FileDeleteOperation) obj;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(reference, other.reference);
        equalsBuilder.append(refFile, other.refFile);
        return equalsBuilder.isEquals();
    }
    
    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append("reference", reference);
        builder.append("refFile", refFile);
        return builder.toString();
    }
}
