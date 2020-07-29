/* Copyright 2017-2020 Norconex Inc.
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
package com.norconex.committer.core3.fs.impl;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.committer.core3.fs.AbstractFSCommitter;
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>
 * Commits documents to XML files.  There are two kinds of document
 * representations: upserts and deletions.
 * </p>
 * <p>
 * If you request to split upserts and deletions into separate files,
 * the generated files will start with "upsert-" (for additions/modifications)
 * and "delete-" (for deletions).
 * </p>
 * <p>
 * The generated files are never updated.  Sending a modified document with the
 * same reference will create a new entry and won't modify any existing ones.
 * You can think of the generated files as a set of commit instructions.
 * </p>
 * <p>
 * The generated XML file names are made of a timestamp and a sequence number.
 * </p>
 * <p>
 * You have the option to give a prefix or suffix to
 * files that will be created (default does not add any).
 * </p>
 *
 * <h3>Generated XML format:</h3>
 * {@nx.xml
 * <docs>
 *   <!-- Document additions: -->
 *   <upsert>
 *     <reference>(document reference, e.g., URL)</reference>
 *     <metadata>
 *       <meta name="(meta field name)">(value)</meta>
 *       <meta name="(meta field name)">(value)</meta>
 *       <!-- meta is repeated for each metadata fields -->
 *     </metadata>
 *     <content>
 *       (document content goes here)
 *     </content>
 *   </upsert>
 *   <upsert>
 *     <!-- upsert element is repeated for each additions -->
 *   </upsert>
 *
 *   <!-- Document deletions: -->
 *   <delete>
 *     <reference>(document reference, e.g., URL)</reference>
 *     <metadata>
 *       <meta name="(meta field name)">(value)</meta>
 *       <meta name="(meta field name)">(value)</meta>
 *       <!-- meta is repeated for each metadata fields -->
 *     </metadata>
 *   </delete>
 *   <delete>
 *     <!-- delete element is repeated for each deletions -->
 *   </delete>
 * </docs>
 * }
 *
 * {@nx.xml.usage
 * <committer class="com.norconex.committer.core3.fs.impl.XMLFileCommitter">
 *   {@nx.include com.norconex.committer.core3.fs.AbstractFSCommitter#options}
 *   <indent>(number of indentation spaces, default does not indent)</indent>
 * </committer>
 * }
 *
 * @author Pascal Essiembre
 * @since 3.0.0 (migrated from 2.0.0)
 */
@SuppressWarnings("javadoc")
public class XMLFileCommitter
        extends AbstractFSCommitter<EnhancedXMLStreamWriter> {

    private int indent = -1;

    public int getIndent() {
        return indent;
    }
    public void setIndent(int indent) {
        this.indent = indent;
    }

    @Override
    protected String getFileExtension() {
        return "xml";
    }
    @Override
    protected EnhancedXMLStreamWriter createDocWriter(Writer writer)
            throws IOException {
        EnhancedXMLStreamWriter xml =
                new EnhancedXMLStreamWriter(writer, false, indent);
        xml.writeStartDocument();
        xml.writeStartElement("docs");
        return xml;
    }
    @Override
    protected void writeUpsert(EnhancedXMLStreamWriter xml,
            UpsertRequest upsertRequest) throws IOException {

        xml.writeStartElement("upsert");

        xml.writeElementString("reference", upsertRequest.getReference());

        xml.writeStartElement("metadata");
        for (Entry<String, List<String>> entry
                : upsertRequest.getMetadata().entrySet()) {
            for (String value : entry.getValue()) {
                xml.writeStartElement("meta");
                xml.writeAttributeString("name", entry.getKey());
                xml.writeCharacters(value);
                xml.writeEndElement();
            }
        }
        xml.writeEndElement();  // </metadata>

        xml.writeElementString("content", IOUtils.toString(
                upsertRequest.getContent(), StandardCharsets.UTF_8).trim());

        xml.writeEndElement();  // </upsert>
    }
    @Override
    protected void writeDelete(EnhancedXMLStreamWriter xml,
            DeleteRequest deleteRequest) throws IOException {

        xml.writeStartElement("delete");

        xml.writeElementString("reference", deleteRequest.getReference());

        xml.writeStartElement("metadata");
        for (Entry<String, List<String>> entry
                : deleteRequest.getMetadata().entrySet()) {
            for (String value : entry.getValue()) {
                xml.writeStartElement("meta");
                xml.writeAttributeString("name", entry.getKey());
                xml.writeCharacters(value);
                xml.writeEndElement(); //meta
            }
        }
        xml.writeEndElement(); // </metadata>

        xml.writeEndElement(); // </delete>
    }
    @Override
    protected void closeDocWriter(EnhancedXMLStreamWriter xml)
            throws IOException {
        if (xml != null) {
            xml.writeEndElement();
            xml.writeEndDocument();
            xml.flush();
            xml.close();
        }
    }

    @Override
    public void loadFSCommitterFromXML(XML xml) {
        setIndent(xml.getInteger("indent", indent));
    }
    @Override
    public void saveFSCommitterToXML(XML xml) {
        xml.addElement("indent", indent);
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
