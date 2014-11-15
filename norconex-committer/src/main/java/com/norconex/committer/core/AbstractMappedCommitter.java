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
package com.norconex.committer.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.Objects;

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

import com.norconex.commons.lang.config.ConfigurationUtil;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.map.Properties;

/**
 * <p>A base class batching documents and offering mappings of source reference
 * and source content fields to target reference and target content fields.  
 * Batched documents are queued on the file system.</p>
 * 
 * <h2>Reference Mapping:</h2>
 * 
 * <h4>Source document reference</h4>
 * 
 * By default the document reference from the source document comes from 
 * document reference value passed to the Committer (obtained internally
 * using {@link IAddOperation#getReference()} 
 * or {@link IDeleteOperation#getReference()}).  
 * If you wish to ignore that original document
 * reference and use a metadata field instead, use the 
 * {@link #setSourceReferenceField(String)} method to do so.
 * 
 * <h4>Target document reference</h4>
 * 
 * The default (or constant) target reference
 * field is for subclasses to define.  
 * 
 * <p />
 * 
 * When both a source and target reference 
 * fields are defined, the source reference field will be deleted unless the 
 * <code>keepReferenceSourceField</code> attribute is set to <code>true</code>. 
 * 
 * <h2>Content Mapping:</h2>
 * 
 * Content typically only occurs when committing additions.
 * 
 * <h4>Source document content</h4>
 * 
 * The default source document content is the actual document content 
 * (obtained internally using {@link IAddOperation#getContentStream()}).  
 * Defining a <code>contentSourceField</code>
 * will use the matching metadata property instead.
 * 
 * <h4>Target document content</h4>
 * 
 * The default (or constant) <b>target content</b> field is for subclasses
 * to define.  
 * 
 * <p />
 * 
 * When both a source and target content fields are defined, the 
 * source content field will be deleted unless the 
 * <code>keepContentSourceField</code> attribute is set to 
 * <code>true</code>. 
 * 
 * <h4>XML Configuration</h4>
 * 
 * <p>Subclasses implementing {@link IXMLConfigurable} should allow this inner 
 * configuration:</p>
 * <pre>
 *      &lt;sourceReferenceField keep="[false|true]"&gt;
 *         (Optional name of field that contains the document reference, when 
 *         the default document reference is not used.  The reference value
 *         will be mapped to the "targetReferenceField" 
 *         specified or target repository default field if one is defined
 *         by the concrete implementation.
 *         Once re-mapped, this metadata source field is 
 *         deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceReferenceField&gt;
 *      &lt;targetReferenceField&gt;
 *         (Name of target repository field where to store a document reference.
 *         If not specified, behavior is defined 
 *         by the concrete implementation.) 
 *      &lt;/targetReferenceField&gt;
 *      &lt;contentSourceField keep="[false|true]"&gt
 *         (If you wish to use a metadata field to act as the document 
 *         "content", you can specify that field here.  Default 
 *         does not take a metadata field but rather the document content.
 *         Once re-mapped, the metadata source field is deleted,
 *         unless "keep" is set to <code>true</code>.)
 *      &lt;/contentSourceField&gt;
 *      &lt;contentTargetField&gt;
 *         (Target repository field name for a document content/body.
 *          Default is defined by concrete implementation.)
 *      &lt;/contentTargetField&gt;
 *      &lt;commitBatchSize&gt;
 *          (max number of documents to send to target repository at once)
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

    private long docCount;

    private String targetReferenceField;
    private String sourceReferenceField;
    private boolean keepReferenceSourceField;
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
    public String getSourceReferenceField() {
        return sourceReferenceField;
    }
    /**
     * sets the source field name holding the unique identifier.
     * @param sourceReferenceField source field name
     */    
    public void setSourceReferenceField(String sourceReferenceField) {
        this.sourceReferenceField = sourceReferenceField;
    }
    /**
     * Gets the target field name to store the unique identifier.
     * @return target field name
     */
    public String getTargetReferenceField() {
        return targetReferenceField;
    }
    /**
     * Sets the target field name to store the unique identifier.
     * @param targetReferenceField target field name
     */
    public void setTargetReferenceField(String targetReferenceField) {
        this.targetReferenceField = targetReferenceField;
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
     * Whether to keep the reference source field or not, once mapped.
     * @return <code>true</code> when keeping source reference field
     */
    public boolean isKeepReferenceSourceField() {
        return keepReferenceSourceField;
    }
    /**
     * Sets whether to keep the ID source field or not, once mapped.
     * @param keepReferenceSourceField <code>true</code> when keeping 
     * source reference field
     */
    public void setKeepReferenceSourceField(boolean keepReferenceSourceField) {
        this.keepReferenceSourceField = keepReferenceSourceField;
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

        //Revise this to match javadoc.

        //--- source reference -> target reference ---
        String referenceValue = operation.getReference();
        if (StringUtils.isNotBlank(sourceReferenceField)) {
            referenceValue = metadata.getString(sourceReferenceField);
        }
        if (StringUtils.isNotBlank(targetReferenceField)) {
            metadata.setString(targetReferenceField, referenceValue);
        }
        if (!keepReferenceSourceField 
                && StringUtils.isNotBlank(sourceReferenceField)
                && StringUtils.isNotBlank(targetReferenceField)
                && !Objects.equals(
                        sourceReferenceField, targetReferenceField)) {
            metadata.remove(sourceReferenceField);
        }

        //--- source content -> target content ---
        if (StringUtils.isNotBlank(contentTargetField)) {
            if (StringUtils.isNotBlank(contentSourceField)) {
                List<String >content = metadata.getStrings(contentSourceField);
                metadata.setString(contentTargetField, 
                        content.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
                if (!keepContentSourceField && !Objects.equals(
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

    @SuppressWarnings("deprecation")
    @Override
    public void saveToXML(Writer out) throws IOException {
        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        try {
            XMLStreamWriter writer = factory.createXMLStreamWriter(out);
            writer.writeStartElement("committer");
            writer.writeAttribute("class", getClass().getCanonicalName());

            if (sourceReferenceField != null) {
                writer.writeStartElement("sourceReferenceField");
                writer.writeAttribute(
                        "keep", Boolean.toString(keepReferenceSourceField));
                writer.writeCharacters(sourceReferenceField);
                writer.writeEndElement();
            }
            if (targetReferenceField != null) {
                writer.writeStartElement("targetReferenceField");
                writer.writeCharacters(targetReferenceField);
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
        XMLConfiguration xml = ConfigurationUtil.newXMLConfiguration(in);
        setSourceReferenceField(xml.getString("sourceReferenceField", sourceReferenceField));
        setKeepReferenceSourceField(xml.getBoolean("sourceReferenceField[@keep]", 
                keepReferenceSourceField));
        setTargetReferenceField(xml.getString("targetReferenceField", targetReferenceField));
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
                .append(sourceReferenceField).append(keepReferenceSourceField)
                .append(targetReferenceField).append(getQueueDir())
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
                .append(sourceReferenceField, other.sourceReferenceField)
                .append(keepReferenceSourceField, other.keepReferenceSourceField)
                .append(targetReferenceField, other.targetReferenceField)
                .append(getCommitBatchSize(), other.getCommitBatchSize())
                .append(getQueueSize(), other.getQueueSize())
                .append(getQueueDir(), other.getQueueDir()).isEquals();
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.appendSuper(super.toString());
        builder.append("docCount", docCount);
        builder.append("targetReferenceField", targetReferenceField);
        builder.append("sourceReferenceField", sourceReferenceField);
        builder.append("keepReferenceSourceField", keepReferenceSourceField);
        builder.append("contentTargetField", contentTargetField);
        builder.append("contentSourceField", contentSourceField);
        builder.append("keepContentSourceField", keepContentSourceField);
        return builder.toString();
    }
}
