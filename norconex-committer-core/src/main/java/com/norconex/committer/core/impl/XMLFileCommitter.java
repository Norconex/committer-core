/* Copyright 2017 Norconex Inc.
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
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.ICommitter;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.config.XMLConfigurationUtil;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;

/**
 * <p>
 * Commits documents to XML files.  There are two kinds of generated files:
 * additions and deletions. 
 * </p>
 * <p>
 * The generated XML file names are made of a timestamp and a sequence number. 
 * The timestamp matches the first time the 
 * {@link #add(String, InputStream, Properties)} or
 * {@link #remove(String, Properties)} methods is called.
 * </p>
 * <p>
 * If you request to split additions and deletions into separate files, 
 * the generated files will start with "add-" (for additions) and "del-" (for 
 * deletions).
 * </p>
 * 
 * <h3>Generated XML format:</h3>
 * <pre>
 * &lt;docs&gt;
 *   &lt;!-- Document additions: --&gt;
 *   &lt;doc-add&gt;
 *     &lt;reference&gt;(document reference, e.g., URL)&lt;/reference&gt;
 *     &lt;metadata&gt;
 *       &lt;meta name="(meta field name)"&gt;(value)&lt;/meta&gt;
 *       &lt;meta name="(meta field name)"&gt;(value)&lt;/meta&gt;
 *       &lt;!-- meta is repeated for each metadata fields --&gt;
 *     &lt;/metadata&gt;
 *     &lt;content&gt;
 *       (document content goes here)
 *     &lt;/content&gt;
 *   &lt;/doc-add&gt;
 *   &lt;doc-add&gt;
 *     &lt;!-- doc-add element is repeated for each additions --&gt;
 *   &lt;/doc-add&gt;
 *   
 *   &lt;!-- Document deletions: --&gt;
 *   &lt;doc-del&gt;
 *     &lt;reference&gt;(document reference, e.g., URL)&lt;/reference&gt;
 *   &lt;/doc-del&gt;
 *   &lt;doc-del&gt;
 *     &lt;!-- doc-del element is repeated for each deletions --&gt;
 *   &lt;/doc-del&gt;
 * &lt;/docs&gt;
 * </pre> 
 * 
 * <h3>XML configuration usage:</h3>
 * <pre>
 *  &lt;committer class="com.norconex.committer.core.impl.XMLFileCommitter"&gt;
 *      &lt;directory&gt;(path where to save XML files)&lt;/directory&gt;
 *      &lt;pretty&gt;[false|true]&lt;/pretty&gt;
 *      &lt;docsPerFile&gt;(max number of docs per XML file)&lt;/docsPerFile&gt;
 *      &lt;compress&gt;[false|true]&lt;/compress&gt;
 *      &lt;splitAddDelete&gt;[false|true]&lt;/splitAddDelete&gt;
 *  &lt;/committer&gt;
 * </pre>
 * 
 * @author Pascal Essiembre
 * @since 2.1.0
 */
public class XMLFileCommitter implements ICommitter, IXMLConfigurable  {

    private static final Logger LOG = 
            LogManager.getLogger(XMLFileCommitter.class);

    /** Default committer directory */
    public static final String DEFAULT_DIRECTORY = "committer-xml";

    //TODO Support oneFilePerThread?

    private String directory = DEFAULT_DIRECTORY;
    private boolean pretty = false;
    private int docsPerFile;
    private boolean compress;
    private boolean splitAddDelete;

    private XMLFile mainXML; // either just for adds or both adds and dels
    private XMLFile delXML;  // for when adds and dels are separated
    private String baseName;
    
    /**
     * Constructor.
     */
    public XMLFileCommitter() {
        super();
    }
    
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
    
    public boolean isPretty() {
        return pretty;
    }
    public void setPretty(boolean indent) {
        this.pretty = indent;
    }
    
    public int getDocsPerFile() {
        return docsPerFile;
    }
    public void setDocsPerFile(int docsPerFile) {
        this.docsPerFile = docsPerFile;
    }

    public boolean isCompress() {
        return compress;
    }
    public void setCompress(boolean compress) {
        this.compress = compress;
    }
    
    public boolean isSplitAddDelete() {
        return splitAddDelete;
    }
    public void setSplitAddDelete(boolean separateAddDelete) {
        this.splitAddDelete = separateAddDelete;
    }

    private synchronized void init() {
        if (baseName == null) {
            baseName = DateFormatUtils.format(
                    System.currentTimeMillis(), "yyyy-MM-dd'T'hh-mm-ss-SSS");
        }
    }

    //TODO only have synchronized if one per thread is NOT enabled
    //Thread.currentThread().getId()
    @Override
    public synchronized void add(
            String reference, InputStream content, Properties metadata) {
        init();
        if (mainXML == null) {
            if (splitAddDelete) {
                mainXML = new XMLFile("add-" + baseName);
            } else {
                mainXML = new XMLFile(baseName);
            }
        }
        mainXML.init();
        try {
            EnhancedXMLStreamWriter xml = mainXML.xml;
            xml.writeStartElement("doc-add");
            xml.writeElementString("reference", reference);
            xml.writeStartElement("metadata");
            for (Entry<String, List<String>> entry : metadata.entrySet()) {
                for (String value : entry.getValue()) {
                    xml.writeStartElement("meta");
                    xml.writeAttributeString("name", entry.getKey());
                    xml.writeCharacters(value);
                    xml.writeEndElement(); //meta
                }
            }
            xml.writeEndElement(); //metadata
            xml.writeStartElement("content");
            xml.writeCharacters(
                    IOUtils.toString(content, StandardCharsets.UTF_8).trim());
            xml.writeEndElement(); //content
            xml.writeEndElement(); //doc
        } catch (XMLStreamException | IOException e) {
            throw new CommitterException(
                    "Cannot write to XML file: " + mainXML.file, e);
        }
        mainXML.docCount++;
        if (docsPerFile > 0 && mainXML.docCount == docsPerFile) {
            mainXML.close();
        }
    }

    @Override
    public synchronized void remove(String reference, Properties metadata) {
        init();
        XMLFile xmlFile;
        if (splitAddDelete) {
            if (delXML == null) {
                delXML = new XMLFile("del-" + baseName);
            }
            xmlFile = delXML;
        } else {
            if (mainXML == null) {
                mainXML = new XMLFile(baseName);
            }
            xmlFile = mainXML;
        }
        
        xmlFile.init();
        try {
            EnhancedXMLStreamWriter xml = xmlFile.xml;
            xml.writeStartElement("doc-del");
            xml.writeElementString("reference", reference);
            xml.writeEndElement(); //doc
        } catch (XMLStreamException e) {
            throw new CommitterException(
                    "Cannot write to XML file: " + xmlFile.file, e);
        }
        xmlFile.docCount++;
        if (docsPerFile > 0 && xmlFile.docCount == docsPerFile) {
            xmlFile.close();
        }
    }

    @Override
    public void commit() {
        if (mainXML != null) {
            mainXML.close();
            mainXML = null;
        }
        if (delXML != null) {
            delXML.close();
            delXML = null;
        }
        baseName = null;
    }

    @Override
    public void loadFromXML(Reader in) {
        XMLConfiguration xml = XMLConfigurationUtil.newXMLConfiguration(in);
        setDirectory(xml.getString("directory", directory));
        setPretty(xml.getBoolean("pretty", pretty));
        setDocsPerFile(xml.getInt("docsPerFile", docsPerFile));
        setCompress(xml.getBoolean("compress", compress));
        setSplitAddDelete(xml.getBoolean("splitAddDelete", splitAddDelete));
    }
    @Override
    public void saveToXML(Writer out) throws IOException {
        try {
            EnhancedXMLStreamWriter writer = new EnhancedXMLStreamWriter(out);
            writer.writeStartElement("committer");
            writer.writeAttribute("class", getClass().getCanonicalName());
            writer.writeElementString("directory", directory);
            writer.writeElementBoolean("pretty", pretty);
            writer.writeElementInteger("docsPerFile", docsPerFile);
            writer.writeElementBoolean("compress", compress);
            writer.writeElementBoolean("splitAddDelete", splitAddDelete);
            writer.writeEndElement();
            writer.flush();
            writer.close();
        } catch (XMLStreamException e) {
            throw new IOException("Cannot save as XML.", e);
        }
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(directory)
                .append(pretty)
                .append(docsPerFile)
                .append(compress)
                .append(splitAddDelete)
                .toHashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof XMLFileCommitter)) {
            return false;
        }
        XMLFileCommitter other = (XMLFileCommitter) obj;
        return new EqualsBuilder()
                .append(directory, other.directory)
                .append(pretty, other.pretty)
                .append(docsPerFile, other.docsPerFile)
                .append(compress, other.compress)
                .append(splitAddDelete, other.splitAddDelete)
                .isEquals();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("directory", directory)
                .append("pretty", pretty)
                .append("docsPerFile", docsPerFile)
                .append("compress", compress)
                .append("splitAddDelete", splitAddDelete)
                .toString();
    }

    private class XMLFile {
        private final File dir;
        private final String fileBaseName;
        private int docCount;
        private int rollCount;
        private File file;
        private EnhancedXMLStreamWriter xml;
        private Writer writer = null;
        public XMLFile(String fileBaseName) {
            super();
            this.dir = new File(directory);
            this.fileBaseName = fileBaseName;
        }
        private void init() {
            if (xml != null) {
                return;
            }
            
            // initialize
            rollCount++;
            String fileName = fileBaseName + "_" + rollCount + ".xml";
            if (compress) {
                fileName += ".gz";
            }
            file = new File(dir, fileName);
            LOG.info("XML File created: " + file);
            try {
                int indentSize = -1;
                if (pretty) {
                    indentSize = 2;
                }

                if (compress) {
                    writer = new OutputStreamWriter(new GZIPOutputStream(
                            new FileOutputStream(file), true));
                } else {
                    writer = new FileWriter(file);
                }
                
                xml = new EnhancedXMLStreamWriter(writer, false, indentSize);
                xml.writeStartDocument();
                xml.writeStartElement("docs");
            } catch (XMLStreamException | IOException e) {
                throw new CommitterException(
                        "Cannot create XML file: " + file, e);
            }
        }
        private void close() {
            if (xml != null) {
                try {
                    xml.writeEndElement();
                    xml.writeEndDocument();
                    xml.flush();
                    xml.close();
                    writer.flush();
                    writer.close();
                } catch (XMLStreamException | IOException e) {
                    throw new CommitterException(
                            "Cannot close XML file: " + file, e);
                }
            }
            xml = null;
            file = null;
            writer = null;
            docCount = 0;
        }
    }
}
