/* Copyright 2010-2018 Norconex Inc.
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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norconex.committer.core.impl.FileSystemCommitter;
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
            LoggerFactory.getLogger(FileAddOperation.class);

    private final String reference;
    private final File contentFile;
    private final File metaFile;
    private final File refFile;
    private final Properties metadata;
    private final int hashCode;

    /**
     * Constructor.
     * @param refFile the reference file to be added
     */
    public FileAddOperation(File refFile) {
        super();
        this.hashCode = refFile.hashCode();
        this.refFile = refFile;

        String basePath = StringUtils.removeEnd(
                refFile.getAbsolutePath(),
                FileSystemCommitter.EXTENSION_REFERENCE);
        this.contentFile = new File(
                basePath + FileSystemCommitter.EXTENSION_CONTENT);
        this.metaFile = new File(
                basePath + FileSystemCommitter.EXTENSION_METADATA);
        try {
            this.reference = FileUtils.readFileToString(
                    refFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new CommitterException(
                    "Could not load reference for " + refFile, e);
        }
        this.metadata = new Properties();
        synchronized (metadata) {
            if (metaFile.exists()) {
                FileInputStream is = null;
                try {
                    is = new FileInputStream(metaFile);
                    metadata.loadFromProperties(is);
                } catch (IOException e) {
                    throw new CommitterException(
                            "Could not load metadata for " + metaFile, e);
                } finally {
                    IOUtils.closeQuietly(is);
                }
            }
        }

    }

    @Override
    public String getReference() {
        return reference;
    }

    @Override
    public void delete() {
        File fileToDelete = null;
        try {
            fileToDelete = metaFile;
            FileUtil.delete(fileToDelete);

            fileToDelete = refFile;
            FileUtil.delete(fileToDelete);

            fileToDelete = contentFile;
            FileUtil.delete(fileToDelete);

        } catch (IOException e) {
            LOG.error("Could not delete commit file: " + fileToDelete, e);
        }
    }

    @Override
    public Properties getMetadata() {
        return metadata;
    }

    @Override
    public InputStream getContentStream() {
        try {
            return new FileInputStream(contentFile);
        } catch (FileNotFoundException e) {
            throw new CommitterException(
                    "Could not obtain content stream for " + contentFile, e);
        }
    }

    @Override
    public int hashCode() {
        return hashCode;
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
        equalsBuilder.append(contentFile, other.contentFile);
        equalsBuilder.append(metadata, other.metadata);
        return equalsBuilder.isEquals();
    }

    @Override
    public String toString() {
        ToStringBuilder builder =
                new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
        builder.append("file", contentFile);
        builder.append("metadata", metadata);
        return builder.toString();
    }
}
