/* Copyright 2010-2017 Norconex Inc.
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
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.time.DateFormatUtils;

import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.ICommitter;
import com.norconex.commons.lang.TimeIdGenerator;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.config.XMLConfigurationUtil;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;


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
 */
@SuppressWarnings("nls")
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
            FileOutputStream out = new FileOutputStream(new File(
                    targetFile.getAbsolutePath() + EXTENSION_METADATA));
            metadata.store(out, "");
            IOUtils.closeQuietly(out);
            
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

    /**
     * Gets the directory where documents to be added are stored.
     * @return directory
     * @deprecated since 2.0.1
     */
    @Deprecated
    public File getAddDir() {
        return new File(directory);
    }
    /**
     * Gets the directory where documents to be removed are stored.
     * @return directory
     * @deprecated since 2.0.1
     */
    @Deprecated
    public File getRemoveDir() {
        return new File(directory); 
    }
    
    private synchronized File createFile(String suffix) throws IOException {
        // Create date directory
        File dateDir = new File(directory, DateFormatUtils.format(
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
    public void loadFromXML(Reader in) {
        XMLConfiguration xml = XMLConfigurationUtil.newXMLConfiguration(in);
        setDirectory(xml.getString("directory", DEFAULT_DIRECTORY));
    }
    @Override
    public void saveToXML(Writer out) throws IOException {
        try {
            EnhancedXMLStreamWriter writer = new EnhancedXMLStreamWriter(out);
            writer.writeStartElement("committer");
            writer.writeAttribute("class", getClass().getCanonicalName());
            writer.writeElementString("directory", directory);
            writer.writeEndElement();
            writer.flush();
            writer.close();
        } catch (XMLStreamException e) {
            throw new IOException("Cannot save as XML.", e);
        }
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder();
        hashCodeBuilder.append(directory);
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
        if (!(obj instanceof FileSystemCommitter)) {
            return false;
        }
        FileSystemCommitter other = (FileSystemCommitter) obj;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(directory, other.directory);
        return equalsBuilder.isEquals();
    }

    @Override
    public String toString() {
        ToStringBuilder builder = 
                new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
        builder.append("directory", directory);
        return builder.toString();
    }
}

