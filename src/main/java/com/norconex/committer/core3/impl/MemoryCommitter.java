/* Copyright 2019-2020 Norconex Inc.
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
package com.norconex.committer.core3.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norconex.committer.core3.AbstractCommitter;
import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.ICommitterRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.text.TextMatcher;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>
 * <b>WARNING: Not intended for production use.</b>
 * </p>
 * <p>
 * A Committer that stores every document received into memory.
 * This can be useful for testing or troubleshooting applications using
 * Committers.
 * Given this committer can eat up memory pretty quickly, its use is strongly
 * discouraged for regular production use.
 * </p>
 *
 * {@nx.xml.usage
 * <committer class="com.norconex.committer.core3.impl.MemoryCommitter">
 *   {@nx.include com.norconex.committer.core3.AbstractCommitter@nx.xml.usage}
 * </committer>
 * }
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
@SuppressWarnings("javadoc")
public class MemoryCommitter extends AbstractCommitter {

    private static final Logger LOG =
            LoggerFactory.getLogger(MemoryCommitter.class);

    private final List<ICommitterRequest> requests = new ArrayList<>();

    private int upsertCount = 0;
    private int deleteCount = 0;

    private boolean ignoreContent;
    private final TextMatcher fieldMatcher = new TextMatcher();

    /**
     * Constructor.
     */
    public MemoryCommitter() {
        super();
    }

    public boolean isIgnoreContent() {
        return ignoreContent;
    }
    public void setIgnoreContent(boolean ignoreContent) {
        this.ignoreContent = ignoreContent;
    }

    public TextMatcher getFieldMatcher() {
        return fieldMatcher;
    }
    public void setFieldMatcher(TextMatcher fieldMatcher) {
        this.fieldMatcher.copyFrom(fieldMatcher);
    }

    @Override
    protected void doInit() {
        //NOOP
    }

    public boolean removeRequest(ICommitterRequest req) {
        return requests.remove(req);
    }

    public List<ICommitterRequest> getAllRequests() {
        return requests;
    }
    public List<UpsertRequest> getUpsertRequests() {
        return requests.stream()
                .filter(o -> o instanceof UpsertRequest)
                .map(o -> (UpsertRequest) o)
                .collect(Collectors.toList());
    }
    public List<DeleteRequest> getDeleteRequests() {
        return requests.stream()
                .filter(o -> o instanceof DeleteRequest)
                .map(o -> (DeleteRequest) o)
                .collect(Collectors.toList());
    }

    public int getUpsertCount() {
        return upsertCount;
    }
    public int getDeleteCount() {
        return deleteCount;
    }
    public int getRequestCount() {
        return requests.size();
    }

    @Override
    protected void doUpsert(UpsertRequest upsertRequest)
            throws CommitterException {
        String memReference = upsertRequest.getReference();
        LOG.debug("Committing upsert request for {}", memReference);

        InputStream memContent = null;
        InputStream reqContent = upsertRequest.getContent();
        if (!ignoreContent && reqContent != null) {
            try {
                memContent = new ByteArrayInputStream(
                        IOUtils.toByteArray(reqContent));
            } catch (IOException e) {
                throw new CommitterException(
                        "Could not do upsert for " + memReference);
            }
        }

        Properties memMetadata = filteredMetadata(upsertRequest.getMetadata());

        requests.add(new UpsertRequest(memReference, memMetadata, memContent));
        upsertCount++;
    }
    @Override
    protected void doDelete(DeleteRequest deleteRequest) {
        String memReference = deleteRequest.getReference();
        LOG.debug("Committing delete request for {}", memReference);
        Properties memMetadata = filteredMetadata(deleteRequest.getMetadata());
        requests.add(new DeleteRequest(memReference, memMetadata));
        deleteCount++;
    }

    private Properties filteredMetadata(Properties reqMetadata) {
        Properties memMetadata = new Properties();
        if (reqMetadata != null) {

            if (fieldMatcher.getPattern() == null) {
                memMetadata.loadFromMap(reqMetadata);
            } else {
                memMetadata.loadFromMap(reqMetadata.entrySet().stream()
                        .filter(en -> fieldMatcher.matches(en.getKey()))
                        .collect(Collectors.toMap(
                                Entry::getKey, Entry::getValue)));
            }
        }
        return memMetadata;
    }

    @Override
    protected void doClose()
            throws com.norconex.committer.core3.CommitterException {
        LOG.info("{} upserts committed.", upsertCount);
        LOG.info("{} deletions committed.", deleteCount);
    }

    @Override
    protected void doClean() throws CommitterException {
        // NOOP
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
        // Cannot use ReflectionToStringBuilder here to prevent
        // "An illegal reflective access operation has occurred"
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .append("requests", requests, false)
                .append("upsertCount", upsertCount)
                .append("deleteCount", deleteCount)
                .build();
    }

    @Override
    public void loadCommitterFromXML(XML xml) {
        //NOOP
    }

    @Override
    public void saveCommitterToXML(XML xml) {
        //NOOP
    }
}
