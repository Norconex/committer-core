/* Copyright 2010-2014 Norconex Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.norconex.committer.core;

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
