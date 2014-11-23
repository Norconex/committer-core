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
package com.norconex.committer.core.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.UUID;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.time.DateFormatUtils;

import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.ICommitter;
import com.norconex.commons.lang.config.ConfigurationUtil;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.map.Properties;


/**
 * Commits a copy of files on the filesystem.  Files are directly saved
 * to the specified directory (no queuing or commit).  Useful for 
 * troubleshooting, or used as a file-based queue implementation by 
 * other committers.
 * <p>
 * XML configuration usage:
 * </p>
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
        File dir = getAddDir();
        if (!dir.exists()) {
            try {
                FileUtils.forceMkdir(dir);
            } catch (IOException e) {
                throw new CommitterException(
                        "Cannot create addition directory: " + dir, e);
            }
        }
        try {
            File targetFile = createFile(dir);

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
                    reference, CharEncoding.UTF_8);
            
        } catch (IOException e) {
            throw new CommitterException(
            		"Cannot queue document addition.  Ref: " + reference, e);
        }
    }
    @Override
    public void remove(String reference, Properties metadata) {
        File dir = getRemoveDir();
        if (!dir.exists()) {
            try {
                FileUtils.forceMkdir(dir);
            } catch (IOException e) {
                throw new CommitterException(
                        "Cannot create removal directory: " + dir, e);
            }
        }
        try {
            File targetFile = createFile(dir);
            FileUtils.writeStringToFile(new File(
                    targetFile.getAbsolutePath() + EXTENSION_REFERENCE),
                    reference, CharEncoding.UTF_8);
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
     */
    public File getAddDir() {
        return new File(directory, "add");
    }
    /**
     * Gets the directory where documents to be removed are stored.
     * @return directory
     */
    public File getRemoveDir() {
        return new File(directory, "remove");
    }
    
    private synchronized File createFile(File dir) throws IOException {

        // Create date directory
        File dateDir = new File(dir, DateFormatUtils.format(
                System.currentTimeMillis(), "yyyy/MM-dd/hh/mm/ss"));
        if (!dateDir.exists()) {
            if (!dateDir.mkdirs()) {
                throw new IOException(
                        "Could not create commit directory: " + dateDir);
            }
        }
        
        // Create file
        return new File(dateDir, UUID.randomUUID().toString());
    }

    @Override
    public void loadFromXML(Reader in) {
        XMLConfiguration xml = ConfigurationUtil.newXMLConfiguration(in);
        setDirectory(xml.getString("directory", DEFAULT_DIRECTORY));
    }
    @Override
    public void saveToXML(Writer out) throws IOException {
        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        try {
            XMLStreamWriter writer = factory.createXMLStreamWriter(out);
            writer.writeStartElement("committer");
            writer.writeAttribute("class", getClass().getCanonicalName());
            writer.writeStartElement("directory");
            writer.writeCharacters(directory);
            writer.writeEndElement();
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
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append("directory", directory);
        return builder.toString();
    }
}

