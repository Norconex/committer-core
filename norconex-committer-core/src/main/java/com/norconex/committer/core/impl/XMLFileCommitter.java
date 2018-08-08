/* Copyright 2017-2018 Norconex Inc.
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
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.ICommitter;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.file.FileUtil;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;
import com.norconex.commons.lang.xml.XML;

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
 * <b>Since 2.1.2</b>, you have to option to give a prefix or suffix to
 * files that will be created (default does not add any).
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
 *      &lt;fileNamePrefix&gt;(optional prefix to created file names)&lt;/fileNamePrefix&gt;
 *      &lt;fileNameSuffix&gt;(optional suffix to created file names)&lt;/fileNameSuffix&gt;
 *  &lt;/committer&gt;
 * </pre>
 *
 * @author Pascal Essiembre
 * @since 2.1.0
 */
public class XMLFileCommitter implements ICommitter, IXMLConfigurable  {

    private static final Logger LOG =
            LoggerFactory.getLogger(XMLFileCommitter.class);

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
    private String fileNamePrefix;
    private String fileNameSuffix;

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

    /**
     * Gets the file name prefix (default is <code>null</code>).
     * @return file name prefix
     * @since 2.1.2
     */
    public String getFileNamePrefix() {
        return fileNamePrefix;
    }
    /**
     * Sets an optional file name prefix.
     * @param fileNamePrefix file name prefix
     * @since 2.1.2
     */
    public void setFileNamePrefix(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
    }

    /**
     * Gets the file name suffix (default is <code>null</code>).
     * @return file name suffix
     * @since 2.1.2
     */
    public String getFileNameSuffix() {
        return fileNameSuffix;
    }
    /**
     * Sets an optional file name suffix.
     * @param fileNameSuffix file name suffix
     * @since 2.1.2
     */
    public void setFileNameSuffix(String fileNameSuffix) {
        this.fileNameSuffix = fileNameSuffix;
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
        } catch (IOException e) {
            mainXML.close();
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
        EnhancedXMLStreamWriter xml = xmlFile.xml;
        xml.writeStartElement("doc-del");
        xml.writeElementString("reference", reference);
        xml.writeEndElement(); //doc
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
    public void loadFromXML(XML xml) {
        setDirectory(xml.getString("directory", directory));
        setPretty(xml.getBoolean("pretty", pretty));
        setDocsPerFile(xml.getInteger("docsPerFile", docsPerFile));
        setCompress(xml.getBoolean("compress", compress));
        setSplitAddDelete(xml.getBoolean("splitAddDelete", splitAddDelete));
        setFileNamePrefix(xml.getString("fileNamePrefix", fileNamePrefix));
        setFileNameSuffix(xml.getString("fileNameSuffix", fileNameSuffix));
    }
    @Override
    public void saveToXML(XML xml) {
        xml.addElement("directory", directory);
        xml.addElement("pretty", pretty);
        xml.addElement("docsPerFile", docsPerFile);
        xml.addElement("compress", compress);
        xml.addElement("splitAddDelete", splitAddDelete);
        xml.addElement("fileNamePrefix", fileNamePrefix);
        xml.addElement("fileNameSuffix", fileNameSuffix);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(directory)
                .append(pretty)
                .append(docsPerFile)
                .append(compress)
                .append(splitAddDelete)
                .append(fileNamePrefix)
                .append(fileNameSuffix)
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
                .append(fileNamePrefix, other.fileNamePrefix)
                .append(fileNameSuffix, other.fileNameSuffix)
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
                .append("fileNamePrefix", fileNamePrefix)
                .append("fileNameSuffix", fileNameSuffix)
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
            String fileName =
                    StringUtils.stripToEmpty(
                            FileUtil.toSafeFileName(fileNamePrefix))
                  + fileBaseName
                  + StringUtils.stripToEmpty(
                          FileUtil.toSafeFileName(fileNameSuffix))
                  + "_" + rollCount + ".xml";
            if (compress) {
                fileName += ".gz";
            }
            file = new File(dir, fileName);
            LOG.info("Creating XML File: " + file);
            try {
                FileUtils.forceMkdir(dir);
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
            } catch (IOException e) {
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
                } catch (IOException e) {
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
