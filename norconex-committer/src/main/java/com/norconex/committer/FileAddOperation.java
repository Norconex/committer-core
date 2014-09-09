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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.commons.lang.file.FileUtil;
import com.norconex.commons.lang.map.Properties;

/**
 * A file-based addition operation.
 * @author Pascal Essiembre
 * @since 1.1.0
 */
public class FileAddOperation implements IAddOperation {

    private static final long serialVersionUID = -7003290965448748871L;
    private static final Logger LOG = 
            LogManager.getLogger(FileAddOperation.class);

    private String reference;
    private final File file;
    private Properties metadata;
    

    /**
     * Constructor.
     * @param file the file to be added
     */
    public FileAddOperation(File file) {
        super();
        this.file = file;
    }

    @Override
    public String getReference() {
        if (reference != null) {
            return reference;
        }
        try {
            reference = FileUtils.readFileToString(
                    new File(file.getAbsolutePath() + ".ref"),
                    CharEncoding.UTF_8);
        } catch (IOException e) {
            throw new CommitterException(
                    "Could not load reference for " + file);
        }
        return reference;
    }
    
    @Override
    public void delete() {
        File fileToDelete = null;
        try {
            fileToDelete = new File(file.getAbsolutePath() + ".meta");
            FileUtil.delete(fileToDelete);

            fileToDelete = new File(file.getAbsolutePath() + ".ref");
            FileUtil.delete(fileToDelete);

            fileToDelete = file;
            FileUtil.delete(fileToDelete);
            
        } catch (IOException e) {
            LOG.error("Could not delete commit file: " + fileToDelete, e);
        }
    }

    @Override
    public Properties getMetadata() {
        if (metadata != null) {
            return metadata;
        }
        metadata = new Properties();
        synchronized (metadata) {
            File metaFile = new File(file.getAbsolutePath() + ".meta");
            if (metaFile.exists()) {
                FileInputStream is = null;
                try {
                    is = new FileInputStream(metaFile);
                    metadata.load(is);
                } catch (IOException e) {
                    throw new CommitterException(
                            "Could not load metadata for " + file, e);
                } finally {
                    IOUtils.closeQuietly(is);
                }
            }
        }
        return metadata;
    }

    @Override
    public InputStream getContentStream() {
        try {
            return new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new CommitterException(
                    "Could not obtain content stream for " + file, e);
        }
    }
    
    @Override
    public int hashCode() {
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder();
        hashCodeBuilder.append(file);
        hashCodeBuilder.append(metadata);
        return hashCodeBuilder.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof FileAddOperation)) {
            return false;
        }
        FileAddOperation other = (FileAddOperation) obj;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(file, other.file);
        equalsBuilder.append(metadata, other.metadata);
        return equalsBuilder.isEquals();
    }
    
    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append("file", file);
        builder.append("metadata", metadata);
        return builder.toString();
    }
}
