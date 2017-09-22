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
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.ICommitter;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.config.XMLConfigurationUtil;
import com.norconex.commons.lang.file.FileUtil;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;

/**
 * <p>
 * Commits documents to JSON files.  There are two kinds of generated files:
 * additions and deletions. 
 * </p>
 * <p>
 * The generated JSON file names are made of a timestamp and a sequence number. 
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
 * <h3>Generated JSON format:</h3>
 * <pre>
 * [
 *   {"doc-add": {
 *     "reference": "document reference, e.g., URL",
 *     "metadata": {
 *       "name": ["value"],
 *       "anothername": [
 *         "multivalue1",
 *         "multivalue2"
 *       ],
 *       "anyname": ["name-value is repeated as necessary"]
 *     },
 *     "content": "Document Content Goes here"
 *   }},
 *   {"doc-add": {
 *     // doc-add is repeated as necessary
 *   }},
 *   {"doc-del": {"reference": "document reference, e.g., URL"}},
 *   {"doc-del": {"reference": "repeated as necessary"}}
 * ]
 * </pre> 
 * 
 * <h3>XML configuration usage:</h3>
 * <pre>
 *  &lt;committer class="com.norconex.committer.core.impl.JSONFileCommitter"&gt;
 *      &lt;directory&gt;(path where to save JSON files)&lt;/directory&gt;
 *      &lt;pretty&gt;[false|true]&lt;/pretty&gt;
 *      &lt;docsPerFile&gt;(max number of docs per JSON file)&lt;/docsPerFile&gt;
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
public class JSONFileCommitter implements ICommitter, IXMLConfigurable  {

    private static final Logger LOG = 
            LogManager.getLogger(JSONFileCommitter.class);

    /** Default committer directory */
    public static final String DEFAULT_DIRECTORY = "committer-json";

    //TODO Support oneFilePerThread?

    private String directory = DEFAULT_DIRECTORY;
    private boolean pretty = false;
    private int docsPerFile;
    private boolean compress;
    private boolean splitAddDelete;

    private JSONFile mainJSON; // either just for adds or both adds and dels
    private JSONFile delJSON;  // for when adds and dels are separated
    private String baseName;
    private String fileNamePrefix;
    private String fileNameSuffix;
    
    /**
     * Constructor.
     */
    public JSONFileCommitter() {
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
            File dir = new File(directory);
            try {
                FileUtils.forceMkdir(dir);
            } catch (IOException e) {
                throw new CommitterException(
                        "Cannot create directory: " + dir.getAbsolutePath(), e);
            }
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
        if (mainJSON == null) {
            if (splitAddDelete) {
                mainJSON = new JSONFile("add-" + baseName);
            } else {
                mainJSON = new JSONFile(baseName);
            }
        }
        mainJSON.init();
        int indent = 0;
        if (pretty) {
            indent = 2;
        }
        
        Writer writer = mainJSON.writer;

        try {
            if (mainJSON.docCount > 0) {
                writer.write(',');
                if (pretty) {
                    writer.write("\n  ");
                }
            }
            JSONObject doc = new JSONObject();
            doc.put("reference", reference);
            doc.put("metadata", metadata);
            doc.put("content", 
                    IOUtils.toString(content, StandardCharsets.UTF_8).trim());
            
            JSONObject docAdd = new JSONObject();
            docAdd.put("doc-add", doc);
            writer.write(docAdd.toString(indent));
        } catch (IOException e) {
            mainJSON.close();
            throw new CommitterException("Cannot write to JSON file: " 
                    + mainJSON.file.getAbsolutePath(), e);
        }

        
        mainJSON.docCount++;
        if (docsPerFile > 0 && mainJSON.docCount == docsPerFile) {
            mainJSON.close();
        }
    }

    @Override
    public synchronized void remove(String reference, Properties metadata) {
        init();
        JSONFile jsonFile;
        if (splitAddDelete) {
            if (delJSON == null) {
                delJSON = new JSONFile("del-" + baseName);
            }
            jsonFile = delJSON;
        } else {
            if (mainJSON == null) {
                mainJSON = new JSONFile(baseName);
            }
            jsonFile = mainJSON;
        }
        
        jsonFile.init();
        
        
        int indent = 0;
        if (pretty) {
            indent = 2;
        }
        
        Writer writer = jsonFile.writer;
        try {
            if (jsonFile.docCount > 0) {
                writer.write(',');
                if (pretty) {
                    writer.write("\n  ");
                }
            }
            JSONObject doc = new JSONObject();
            doc.put("reference", reference);

            JSONObject docDel = new JSONObject();
            docDel.put("doc-del", doc);
            
            writer.write(docDel.toString(indent));
        } catch (IOException e) {
            jsonFile.close();
            throw new CommitterException("Cannot write to JSON file: " 
                    + mainJSON.file.getAbsolutePath(), e);
        }
        jsonFile.docCount++;
        if (docsPerFile > 0 && jsonFile.docCount == docsPerFile) {
            jsonFile.close();
        }
    }

    @Override
    public void commit() {
        if (mainJSON != null) {
            mainJSON.close();
            mainJSON = null;
        }
        if (delJSON != null) {
            delJSON.close();
            delJSON = null;
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
        setFileNamePrefix(xml.getString("fileNamePrefix", fileNamePrefix));
        setFileNameSuffix(xml.getString("fileNameSuffix", fileNameSuffix));
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
            writer.writeElementString("fileNamePrefix", fileNamePrefix);
            writer.writeElementString("fileNameSuffix", fileNameSuffix);
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
        if (!(obj instanceof JSONFileCommitter)) {
            return false;
        }
        JSONFileCommitter other = (JSONFileCommitter) obj;
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

    private class JSONFile {
        private final File dir;
        private final String fileBaseName;
        private int docCount;
        private int rollCount;
        private File file;
        private Writer writer;
        public JSONFile(String fileBaseName) {
            super();
            this.dir = new File(directory);
            this.fileBaseName = fileBaseName;
        }
        private void init() {
            if (writer != null) {
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
                  + "_" + rollCount + ".json";
            if (compress) {
                fileName += ".gz";
            }
            file = new File(dir, fileName);
            LOG.info("JSON File created: " + file);
            try {
                if (compress) {
                    writer = new OutputStreamWriter(new GZIPOutputStream(
                            new FileOutputStream(file), true));
                } else {
                    writer = new FileWriter(file);
                }
                writer.write("[");
                if (pretty) {
                    writer.write("\n  ");
                }
            } catch (IOException e) {
                throw new CommitterException(
                        "Cannot create JSON file: " + file, e);
            }
        }
        private void close() {
            if (writer != null) {
                try {
                    if (pretty) {
                        writer.write("\n");
                    }
                    writer.write("]");
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    throw new CommitterException(
                            "Cannot close JSON file: " + file, e);
                }
            }
            file = null;
            writer = null;
            docCount = 0;
        }
    }
}
