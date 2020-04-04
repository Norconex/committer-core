/* Copyright 2020 Norconex Inc.
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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.json.JSONObject;

import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.committer.core3.fs.AbstractFSCommitter;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>
 * Commits documents to JSON files.  There are two kinds of document
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
 * The generated JSON file names are made of a timestamp and a sequence number.
 * </p>
 * <p>
 * You have the option to give a prefix or suffix to
 * files that will be created (default does not add any).
 * </p>
 *
 * <h3>Generated JSON format:</h3>
 * {@nx.json
 * [
 *   {"upsert": {
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
 *   {"upsert": {
 *     // upsert is repeated as necessary
 *   }},
 *   {"delete": {
 *     "reference": "document reference, e.g., URL",
 *     "metadata": {
 *       "name": ["value"],
 *       "anothername": [
 *         "multivalue1",
 *         "multivalue2"
 *       ],
 *       "anyname": ["name-value is repeated as necessary"]
 *     }
 *   }},
 *   {"delete": {
 *     // delete is repeated as necessary
 *   }}
 * ]
 *
 * }
 *
 * {@nx.xml.usage
 * <committer class="com.norconex.committer.core.impl.JSONFileCommitter">
 *   {@nx.include com.norconex.committer.core3.fs.AbstractFSCommitter#options}
 *   <indent>(number of indentation spaces, default does not indent)</indent>
 * </committer>
 * }
 *
 * @author Pascal Essiembre
 * @since 3.0.0 (migrated from 2.0.0)
 */
@SuppressWarnings("javadoc")
public class JSONFileCommitter extends AbstractFSCommitter<Writer> {

    private int indent = -1;

    private boolean first = true;

    public int getIndent() {
        return indent;
    }
    public void setIndent(int indent) {
        this.indent = indent;
    }

    @Override
    protected String getFileExtension() {
        return "json";
    }
    @Override
    protected Writer createDocWriter(Writer writer) throws IOException {
        writer.write("[");
        newLine(writer);
        return writer;
    }
    @Override
    protected void writeUpsert(
            Writer writer, UpsertRequest upsertRequest) throws IOException {

        if (!first) {
            writer.write(',');
            newLine(writer);
        }

        JSONObject doc = new JSONObject();
        doc.put("reference", upsertRequest.getReference());
        doc.put("metadata", new JSONObject(upsertRequest.getMetadata()));
        doc.put("content", IOUtils.toString(
                upsertRequest.getContent(), StandardCharsets.UTF_8).trim());

        JSONObject upsertObj = new JSONObject();
        upsertObj.put("upsert", doc);
        if (indent > -1) {
            writer.write(upsertObj.toString(indent));
        } else {
            writer.write(upsertObj.toString());
        }

        first = false;
    }

    @Override
    protected void writeDelete(
            Writer writer, DeleteRequest deleteRequest) throws IOException {

        if (!first) {
            writer.write(',');
            newLine(writer);
        }

        JSONObject doc = new JSONObject();
        doc.put("reference", deleteRequest.getReference());
        doc.put("metadata", new JSONObject(deleteRequest.getMetadata()));

        JSONObject deleteObj = new JSONObject();
        deleteObj.put("delete", doc);
        if (indent > -1) {
            writer.write(deleteObj.toString(indent));
        } else {
            writer.write(deleteObj.toString());
        }

        first = false;
    }

    @Override
    protected void closeDocWriter(Writer writer)
            throws IOException {
        if (writer != null) {
            if (indent > -1) {
                writer.write("\n");
            }
            writer.write("]");
            writer.close();
        }
    }

    private void newLine(Writer writer) throws IOException {
        if (indent > -1) {
            writer.write('\n');
        }
    }

    @Override
    public void loadCommitterFromXML(XML xml) {
        setIndent(xml.getInteger("indent", indent));
    }
    @Override
    public void saveCommitterToXML(XML xml) {
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
