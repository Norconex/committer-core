/* Copyright 2010-2013 Norconex Inc.
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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.norconex.commons.lang.config.ConfigurationLoader;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.map.Properties;

/**
 * <p>A base class batching documents and offering mappings of source id and 
 * source content fields to target id and target content fields.  
 * Batched documents are queued on the file system.</p>
 * 
 * <h4>ID Mapping:</h4>
 * 
 * <p>Both the <code>idSourceField</code> and <code>idTargetField</code> must 
 * be set for ID mapping to take place. The default <b>source id</b> field is 
 * the metadata normally set by the Norconex Importer module called 
 * <code>document.reference</code>.  The default (or constant) <b>target id</b> 
 * field is for subclasses to define.  When an ID mapping is defined, the 
 * source id field will be deleted unless the <code>keepIdSourceField</code>
 * attribute is set to <code>true</code>.</p> 
 * 
 * <h4>Content Mapping:</h4>
 * 
 * <p>Only the <code>contentTargetField</code> needs to be set for content
 * mapping to take place.   The default <b>source content</b> is
 * the actual document content.  Defining a <code>contentSourceField</code>
 * will use the matching metadata property instead.
 * The default (or constant) <b>target content</b> field is for subclasses
 * to define.  When a content mapping is defined, the 
 * source content field will be deleted (if provided) unless the 
 * <code>keepContentSourceField</code> attribute is set to 
 * <code>true</code>.</p> 
 * 
 * <p>Subclasses implementing {@link IXMLConfigurable} should allow this inner 
 * configuration:</p>
 * <pre>
 *      &lt;idSourceField keep="[false|true]"&gt;
 *         (Name of source field that will be mapped to the IDOL "DREREFERENCE"
 *         field or whatever "idTargetField" specified.
 *         Default is the document reference metadata field: 
 *         "document.reference".  Once re-mapped, the metadata source field is 
 *         deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/idSourceField&gt;
 *      &lt;idTargetField&gt;
 *         (Name of IDOL target field where to store a document unique 
 *         identifier (idSourceField).  If not specified, default 
 *         is "DREREFERENCE".) 
 *      &lt;/idTargetField&gt;
 *      &lt;contentSourceField keep="[false|true]&gt";
 *         (If you wish to use a metadata field to act as the document 
 *         "content", you can specify that field here.  Default 
 *         does not take a metadata field but rather the document content.
 *         Once re-mapped, the metadata source field is deleted,
 *         unless "keep" is set to <code>true</code>.)
 *      &lt;/contentSourceField&gt;
 *      &lt;contentTargetField&gt;
 *         (IDOL target field name for a document content/body.
 *          Default is: DRECONTENT)
 *      &lt;/contentTargetField&gt;
 *      &lt;commitBatchSize&gt;
 *          (max number of documents to send IDOL at once)
 *      &lt;/commitBatchSize&gt;
 *      &lt;queueDir&gt;(optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueSize&gt;(max queue size before committing)&lt;/queueSize&gt;
 *      &lt;maxRetries&gt;(max retries upon commit failures)&lt;/maxRetries&gt;
 *      &lt;maxRetryWait&gt;(max delay between retries)&lt;/maxRetryWait&gt;
 * </pre>
 * 
 * @author Pascal Essiembre
 * @author Pascal Dimassimo
 * @since 1.1.0
 */
@SuppressWarnings("nls")
public abstract class AbstractMappedCommitter
        extends AbstractBatchCommitter implements IXMLConfigurable {

    private static final long serialVersionUID = 5437833425204155264L;

    private long docCount;

    private String idTargetField;
    private String idSourceField = DEFAULT_DOCUMENT_REFERENCE;
    private boolean keepIdSourceField;
    private String contentTargetField;
    private String contentSourceField;
    private boolean keepContentSourceField;

    /**
     * Creates a new instance.
     */
    public AbstractMappedCommitter() {
        super();
    }
    /**
     * Creates a new instance with given commit batch size.
     * @param commitBatchSize commit batch size
     */
    public AbstractMappedCommitter(int commitBatchSize) {
        super(commitBatchSize);
    }

    /**
     * Gets the source field name holding the unique identifier.
     * @return source field name
     */
    public String getIdSourceField() {
        return idSourceField;
    }
    /**
     * sets the source field name holding the unique identifier.
     * @param idSourceField source field name
     */    
    public void setIdSourceField(String idSourceField) {
        this.idSourceField = idSourceField;
    }
    /**
     * Gets the target field name to store the unique identifier.
     * @return target field name
     */
    public String getIdTargetField() {
        return idTargetField;
    }
    /**
     * Sets the target field name to store the unique identifier.
     * @param idTargetField target field name
     */
    public void setIdTargetField(String idTargetField) {
        this.idTargetField = idTargetField;
    }
    /**
     * Gets the target field where to store the document content.
     * @return target field name
     */
    public String getContentTargetField() {
        return contentTargetField;
    }
    /**
     * Sets the target field where to store the document content.
     * @param contentTargetField target field name
     */
    public void setContentTargetField(String contentTargetField) {
        this.contentTargetField = contentTargetField;
    }
    /**
     * Gets the source field name holding the document content.
     * @return source field name
     */
    public String getContentSourceField() {
        return contentSourceField;
    }
    /**
     * Sets the source field name holding the document content.
     * @param contentSourceField source field name
     */
    public void setContentSourceField(String contentSourceField) {
        this.contentSourceField = contentSourceField;
    }
    /**
     * Whether to keep the ID source field or not, once mapped.
     * @return <code>true</code> when keeping ID source field
     */
    public boolean isKeepIdSourceField() {
        return keepIdSourceField;
    }
    /**
     * Sets whether to keep the ID source field or not, once mapped.
     * @param keepIdSourceField <code>true</code> when keeping ID source field
     */
    public void setKeepIdSourceField(boolean keepIdSourceField) {
        this.keepIdSourceField = keepIdSourceField;
    }
    /**
     * Whether to keep the content source field or not, once mapped.
     * @return <code>true</code> when keeping content source field
     */
    public boolean isKeepContentSourceField() {
        return keepContentSourceField;
    }
    /**
     * Sets whether to keep the content source field or not, once mapped.
     * @param keepContentSourceField <code>true</code> when keeping content 
     * source field
     */
    public void setKeepContentSourceField(boolean keepContentSourceField) {
        this.keepContentSourceField = keepContentSourceField;
    }


    @Override
    protected void prepareCommitAddition(IAddOperation operation)
            throws IOException {
        Properties metadata = operation.getMetadata();

        //--- source ID -> target ID ---
        if (StringUtils.isNotBlank(idSourceField)
                && StringUtils.isNotBlank(idTargetField)) {
            metadata.setString(idTargetField, 
                    metadata.getString(idSourceField));
            if (!keepIdSourceField 
                    && !ObjectUtils.equals(idSourceField, idTargetField)) {
                metadata.remove(idSourceField);
            }
        }
        
        //--- source content -> target content ---
        if (StringUtils.isNotBlank(contentTargetField)) {
            if (StringUtils.isNotBlank(contentSourceField)) {
                List<String >content = metadata.getStrings(contentSourceField);
                metadata.setString(contentTargetField, 
                        content.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
                if (!keepContentSourceField && !ObjectUtils.equals(
                        contentSourceField, contentTargetField)) {
                    metadata.remove(contentSourceField);
                }
            } else {
                InputStream is = operation.getContentStream();
                metadata.setString(contentTargetField, IOUtils.toString(is));
                IOUtils.closeQuietly(is);
            }
        }
    }

    @Override
    public void saveToXML(Writer out) throws IOException {
        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        try {
            XMLStreamWriter writer = factory.createXMLStreamWriter(out);
            writer.writeStartElement("committer");
            writer.writeAttribute("class", getClass().getCanonicalName());

            if (idSourceField != null) {
                writer.writeStartElement("idSourceField");
                writer.writeAttribute(
                        "keep", Boolean.toString(keepIdSourceField));
                writer.writeCharacters(idSourceField);
                writer.writeEndElement();
            }
            if (idTargetField != null) {
                writer.writeStartElement("idTargetField");
                writer.writeCharacters(idTargetField);
                writer.writeEndElement();
            }
            if (contentSourceField != null) {
                writer.writeStartElement("contentSourceField");
                writer.writeAttribute("keep",
                        Boolean.toString(keepContentSourceField));
                writer.writeCharacters(contentSourceField);
                writer.writeEndElement();
            }
            if (contentTargetField != null) {
                writer.writeStartElement("contentTargetField");
                writer.writeCharacters(contentTargetField);
                writer.writeEndElement();
            }
            if (getQueueDir() != null) {
                writer.writeStartElement("queueDir");
                writer.writeCharacters(getQueueDir());
                writer.writeEndElement();
            }
            writer.writeStartElement("queueSize");
            writer.writeCharacters(ObjectUtils.toString(getQueueSize()));
            writer.writeEndElement();

            writer.writeStartElement("commitBatchSize");
            writer.writeCharacters(ObjectUtils.toString(getCommitBatchSize()));
            writer.writeEndElement();

            writer.writeStartElement("maxRetries");
            writer.writeCharacters(ObjectUtils.toString(getMaxRetries()));
            writer.writeEndElement();

            writer.writeStartElement("maxRetryWait");
            writer.writeCharacters(ObjectUtils.toString(getMaxRetryWait()));
            writer.writeEndElement();

            saveToXML(writer);

            writer.writeEndElement();
            writer.flush();
            writer.close();
        } catch (XMLStreamException e) {
            throw new IOException("Cannot save as XML.", e);
        }
    }

    /**
     * Allows subclasses to write their config to xml
     * 
     * @param writer the xml being written
     * @throws XMLStreamException
     */
    protected abstract void saveToXML(XMLStreamWriter writer)
            throws XMLStreamException;

    @Override
    public void loadFromXML(Reader in) {
        XMLConfiguration xml = ConfigurationLoader.loadXML(in);
        setIdSourceField(xml.getString("idSourceField", idSourceField));
        setKeepIdSourceField(xml.getBoolean("idSourceField[@keep]", 
                keepIdSourceField));
        setIdTargetField(xml.getString("idTargetField", idTargetField));
        setContentSourceField(
                xml.getString("contentSourceField", contentSourceField));
        setKeepContentSourceField(xml.getBoolean("contentSourceField[@keep]", 
                keepContentSourceField));
        setContentTargetField(
                xml.getString("contentTargetField", contentTargetField));
        setQueueDir(xml.getString("queueDir", DEFAULT_QUEUE_DIR));
        setQueueSize(xml.getInt("queueSize", 
                AbstractBatchCommitter.DEFAULT_QUEUE_SIZE));
        setCommitBatchSize(xml.getInt("commitBatchSize", 
                AbstractBatchCommitter.DEFAULT_COMMIT_BATCH_SIZE));
        setMaxRetries(xml.getInt("maxRetries", 0));
        setMaxRetryWait(xml.getInt("maxRetryWait", 0));

        loadFromXml(xml);
    }

    /**
     * Allows subclasses to load their config from xml
     * 
     * @param xml
     */
    protected abstract void loadFromXml(XMLConfiguration xml);

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .appendSuper(super.hashCode())
                .append(contentSourceField)
                .append(keepContentSourceField).append(contentTargetField)
                .append(idSourceField).append(keepIdSourceField)
                .append(idTargetField).append(getQueueDir())
                .append(getQueueSize()).append(getCommitBatchSize())
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
        if (!(obj instanceof AbstractMappedCommitter)) {
            return false;
        }
        AbstractMappedCommitter other = (AbstractMappedCommitter) obj;
        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(contentSourceField, other.contentSourceField)
                .append(keepContentSourceField, other.keepContentSourceField)
                .append(contentTargetField, other.contentTargetField)
                .append(idSourceField, other.idSourceField)
                .append(keepIdSourceField, other.keepIdSourceField)
                .append(idTargetField, other.idTargetField)
                .append(getCommitBatchSize(), other.getCommitBatchSize())
                .append(getQueueSize(), other.getQueueSize())
                .append(getQueueDir(), other.getQueueDir()).isEquals();
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.appendSuper(super.toString());
        builder.append("docCount", docCount);
        builder.append("idTargetField", idTargetField);
        builder.append("idSourceField", idSourceField);
        builder.append("keepIdSourceField", keepIdSourceField);
        builder.append("contentTargetField", contentTargetField);
        builder.append("contentSourceField", contentSourceField);
        builder.append("keepContentSourceField", keepContentSourceField);
        return builder.toString();
    }
}
