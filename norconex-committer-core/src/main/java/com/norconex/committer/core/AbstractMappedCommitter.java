/* Copyright 2010-2016 Norconex Inc.
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
import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.norconex.commons.lang.config.ConfigurationUtil;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.map.Properties;

/**
 * <p>A base class batching documents and offering mappings of source reference
 * and source content fields to target reference and target content fields.  
 * Batched documents are queued on the file system.</p>
 * 
 * <h3>Reference Mapping:</h3>
 * 
 * <h4>Source document reference</h4>
 * 
 * <p>By default the document reference from the source document comes from 
 * document reference value passed to the Committer (obtained internally
 * using {@link IAddOperation#getReference()} 
 * or {@link IDeleteOperation#getReference()}).  
 * If you wish to ignore that original document
 * reference and use a metadata field instead, use the 
 * {@link #setSourceReferenceField(String)} method to do so.</p>
 * 
 * <h4>Target document reference</h4>
 * 
 * <p>The default (or constant) target reference
 * field is for subclasses to define.</p>
 * 
 * <p>When both a source and target reference 
 * fields are defined, the source reference field will be deleted unless the 
 * <code>keepSourceReferenceField</code> attribute is set to <code>true</code>. 
 * </p>
 * 
 * <h3>Content Mapping:</h3>
 * 
 * <p>Content typically only occurs when committing additions.</p>
 * 
 * <h4>Source document content</h4>
 * 
 * <p>The default source document content is the actual document content 
 * (obtained internally using {@link IAddOperation#getContentStream()}).  
 * Defining a <code>sourceContentField</code>
 * will use the matching metadata property instead.</p>
 * 
 * <h4>Target document content</h4>
 * 
 * <p>The default (or constant) <b>target content</b> field is for subclasses
 * to define.</p> 
 * 
 * <p>When both a source and target content fields are defined, the 
 * source content field will be deleted unless the 
 * <code>keepSourceContentField</code> attribute is set to 
 * <code>true</code>.</p>
 * 
 * <a id="xml-config"></a>
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
 *      &lt;sourceContentField keep="[false|true]"&gt;
 *         (If you wish to use a metadata field to act as the document 
 *         "content", you can specify that field here.  Default 
 *         does not take a metadata field but rather the document content.
 *         Once re-mapped, the metadata source field is deleted,
 *         unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceContentField&gt;
 *      &lt;targetContentField&gt;
 *         (Target repository field name for a document content/body.
 *          Default is defined by concrete implementation.)
 *      &lt;/targetContentField&gt;
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

    // Source fields
    private String sourceReferenceField;
    private String sourceContentField;
    private boolean keepSourceReferenceField;
    private boolean keepSourceContentField;

    // Target fields
    private String targetReferenceField;
    private String targetContentField;

    
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
    public String getTargetContentField() {
        return targetContentField;
    }
    /**
     * Sets the target field where to store the document content.
     * @param targetContentField target field name
     */
    public void setTargetContentField(String targetContentField) {
        this.targetContentField = targetContentField;
    }
    /**
     * Gets the source field name holding the document content.
     * @return source field name
     */
    public String getSourceContentField() {
        return sourceContentField;
    }
    /**
     * Sets the source field name holding the document content.
     * @param sourceContentField source field name
     */
    public void setSourceContentField(String sourceContentField) {
        this.sourceContentField = sourceContentField;
    }
    /**
     * Whether to keep the reference source field or not, once mapped.
     * @return <code>true</code> when keeping source reference field
     */
    public boolean isKeepSourceReferenceField() {
        return keepSourceReferenceField;
    }
    /**
     * Sets whether to keep the ID source field or not, once mapped.
     * @param keepSourceReferenceField <code>true</code> when keeping 
     * source reference field
     */
    public void setKeepSourceReferenceField(boolean keepSourceReferenceField) {
        this.keepSourceReferenceField = keepSourceReferenceField;
    }
    /**
     * Whether to keep the content source field or not, once mapped.
     * @return <code>true</code> when keeping content source field
     */
    public boolean isKeepSourceContentField() {
        return keepSourceContentField;
    }
    /**
     * Sets whether to keep the content source field or not, once mapped.
     * @param keepSourceContentField <code>true</code> when keeping content 
     * source field
     */
    public void setKeepSourceContentField(boolean keepSourceContentField) {
        this.keepSourceContentField = keepSourceContentField;
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
        if (!keepSourceReferenceField 
                && StringUtils.isNotBlank(sourceReferenceField)
                && StringUtils.isNotBlank(targetReferenceField)
                && !Objects.equals(
                        sourceReferenceField, targetReferenceField)) {
            metadata.remove(sourceReferenceField);
        }

        //--- source content -> target content ---
        if (StringUtils.isNotBlank(targetContentField)) {
            if (StringUtils.isNotBlank(sourceContentField)) {
                List<String >content = metadata.getStrings(sourceContentField);
                metadata.setString(targetContentField, 
                        content.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
                if (!keepSourceContentField && !Objects.equals(
                        sourceContentField, targetContentField)) {
                    metadata.remove(sourceContentField);
                }
            } else {
                InputStream is = operation.getContentStream();
                metadata.setString(targetContentField, 
                        IOUtils.toString(is, CharEncoding.UTF_8));
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
                        "keep", Boolean.toString(keepSourceReferenceField));
                writer.writeCharacters(sourceReferenceField);
                writer.writeEndElement();
            }
            if (targetReferenceField != null) {
                writer.writeStartElement("targetReferenceField");
                writer.writeCharacters(targetReferenceField);
                writer.writeEndElement();
            }
            if (sourceContentField != null) {
                writer.writeStartElement("sourceContentField");
                writer.writeAttribute("keep",
                        Boolean.toString(keepSourceContentField));
                writer.writeCharacters(sourceContentField);
                writer.writeEndElement();
            }
            if (targetContentField != null) {
                writer.writeStartElement("targetContentField");
                writer.writeCharacters(targetContentField);
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
     * @throws XMLStreamException problem saving to XML
     */
    protected abstract void saveToXML(XMLStreamWriter writer)
            throws XMLStreamException;

    @Override
    public void loadFromXML(Reader in) {
        XMLConfiguration xml = ConfigurationUtil.newXMLConfiguration(in);
        setSourceReferenceField(xml.getString("sourceReferenceField", sourceReferenceField));
        setKeepSourceReferenceField(xml.getBoolean("sourceReferenceField[@keep]", 
                keepSourceReferenceField));
        setTargetReferenceField(xml.getString("targetReferenceField", targetReferenceField));
        setSourceContentField(
                xml.getString("sourceContentField", sourceContentField));
        setKeepSourceContentField(xml.getBoolean("sourceContentField[@keep]", 
                keepSourceContentField));
        setTargetContentField(
                xml.getString("targetContentField", targetContentField));
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
     * @param xml XML configuration
     */
    protected abstract void loadFromXml(XMLConfiguration xml);

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .appendSuper(super.hashCode())
                .append(sourceContentField)
                .append(keepSourceContentField)
                .append(targetContentField)
                .append(sourceReferenceField)
                .append(keepSourceReferenceField)
                .append(targetReferenceField)
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
                .append(sourceContentField, other.sourceContentField)
                .append(keepSourceContentField, other.keepSourceContentField)
                .append(targetContentField, other.targetContentField)
                .append(sourceReferenceField, other.sourceReferenceField)
                .append(keepSourceReferenceField, other.keepSourceReferenceField)
                .append(targetReferenceField, other.targetReferenceField)
                .isEquals();
    }

    @Override
    public String toString() {
        ToStringBuilder builder = 
                new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
        builder.appendSuper(super.toString());
        builder.append("targetReferenceField", targetReferenceField);
        builder.append("sourceReferenceField", sourceReferenceField);
        builder.append("keepSourceReferenceField", keepSourceReferenceField);
        builder.append("targetContentField", targetContentField);
        builder.append("sourceContentField", sourceContentField);
        builder.append("keepSourceContentField", keepSourceContentField);
        return builder.toString();
    }
}
