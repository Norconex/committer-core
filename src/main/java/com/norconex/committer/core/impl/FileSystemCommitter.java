/* Copyright 2010-2020 Norconex Inc.
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
package com.norconex.committer.core.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.time.DateFormatUtils;

import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.ICommitter;
import com.norconex.commons.lang.TimeIdGenerator;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.xml.IXMLConfigurable;
import com.norconex.commons.lang.xml.XML;


/**
 * <p>
 * Commits a copy of files on the filesystem.  Files are directly saved
 * to the specified directory (no queuing or commit).  Useful for
 * troubleshooting, or used as a file-based queue implementation by
 * other committers.
 * </p>
 * <h3>XML configuration usage:</h3>
 * <pre>
 *  &lt;committer class="com.norconex.committer.core.impl.FileSystemCommitter"&gt;
 *      &lt;directory&gt;(path where to save files)&lt;/directory&gt;
 *  &lt;/committer&gt;
 * </pre>
 * @author Pascal Essiembre
 * @deprecated Since 3.0.0.
 */
@Deprecated
public class FileSystemCommitter implements ICommitter, IXMLConfigurable {

    /** Default committer directory */
    public static final String DEFAULT_DIRECTORY = "committer";

    public static final String EXTENSION_CONTENT = ".cntnt";
    public static final String EXTENSION_METADATA = ".meta";
    public static final String EXTENSION_REFERENCE = ".ref";

    public static final String FILE_SUFFIX_ADD = "-add";
    public static final String FILE_SUFFIX_REMOVE = "-del";

    private String directory = DEFAULT_DIRECTORY;

    /**
     * Gets the directory where files are committed.
     * @return directory
     */
    public String getDirectory() {
        return directory;
    }
    /**
     * Sets the directory where files are committed.
     * @param directory the directory
     */
    public void setDirectory(String directory) {
        this.directory = directory;
    }

    @Override
    public void add(
            String reference, InputStream content, Properties metadata) {
        try {
            File targetFile = createFile(FILE_SUFFIX_ADD);

            // Content
            FileUtils.copyInputStreamToFile(content,
                    new File(targetFile.getAbsolutePath() + EXTENSION_CONTENT));

            // Metadata
            try (FileOutputStream out = new FileOutputStream(new File(
                    targetFile.getAbsolutePath() + EXTENSION_METADATA))) {
                metadata.storeToProperties(out, "");
            }

            // Reference
            FileUtils.writeStringToFile(new File(
                    targetFile.getAbsolutePath() + EXTENSION_REFERENCE),
                    reference, StandardCharsets.UTF_8);

        } catch (IOException e) {
            throw new CommitterException(
                    "Cannot queue document addition.  Ref: " + reference, e);
        }
    }
    @Override
    public void remove(String reference, Properties metadata) {
        try {
            File targetFile = createFile(FILE_SUFFIX_REMOVE);
            FileUtils.writeStringToFile(new File(
                    targetFile.getAbsolutePath() + EXTENSION_REFERENCE),
                    reference, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new CommitterException(
                    "Cannot queue document removal.  Ref: " + reference, e);
        }
    }

    @Override
    public void commit() {
        //DO NOTHING
    }

    private synchronized File createFile(String suffix) {
        // Create date directory
        String safeDir =
                ObjectUtils.defaultIfNull(directory, DEFAULT_DIRECTORY);
        File dateDir = new File(safeDir, DateFormatUtils.format(
                System.currentTimeMillis(), "yyyy/MM-dd/hh/mm/ss"));
        if (!dateDir.exists()) {
            try {
                FileUtils.forceMkdir(dateDir);
            } catch (IOException e) {
                throw new CommitterException(
                        "Cannot create commit directory: " + dateDir, e);
            }
        }
        // Create file
        return new File(dateDir, TimeIdGenerator.next() + suffix);
    }

    @Override
    public void loadFromXML(XML xml) {
        setDirectory(xml.getString("directory", directory));
    }
    @Override
    public void saveToXML(XML xml) {
        xml.addElement("directory", directory);
    }

    @Override
    public boolean equals(final Object other) {
        return EqualsBuilder.reflectionEquals(this, other);
    }
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
    @Override
    public String toString() {
        return new ReflectionToStringBuilder(
                this, ToStringStyle.SHORT_PREFIX_STYLE).toString();
    }
}

