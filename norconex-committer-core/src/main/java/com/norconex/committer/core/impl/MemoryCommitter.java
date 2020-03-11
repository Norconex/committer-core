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
package com.norconex.committer.core.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringExclude;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.builder.ToStringSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.ICommitter;
import com.norconex.committer.core.IDeleteOperation;
import com.norconex.commons.lang.map.Properties;

/**
 * <b>WARNING: Not intended for production use.</b>
 *
 * <p>
 * A Committer that stores every document received into memory.
 * This can be useful for testing or troubleshooting applications using
 * Committers.
 * Given this committer can eat up memory pretty quickly, its use is strongly
 * discouraged for regular production use.
 * </p>
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public class MemoryCommitter implements ICommitter  {

    private static final Logger LOG =
            LoggerFactory.getLogger(MemoryCommitter.class);

    @ToStringSummary
    private final List<ICommitOperation> operations = new ArrayList<>();

    private long addCount = 0;
    private long removeCount = 0;

    @ToStringExclude
    private boolean ignoreContent;
    @ToStringExclude
    private String fieldsRegex;

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

    public String getFieldsRegex() {
        return fieldsRegex;
    }
    public void setFieldsRegex(String fieldsRegex) {
        this.fieldsRegex = fieldsRegex;
    }

    public List<ICommitOperation> getAllOperations() {
        return operations;
    }
    public List<IAddOperation> getAddOperations() {
        return operations.stream()
                .filter(o -> o instanceof IAddOperation)
                .map(o -> (IAddOperation) o)
                .collect(Collectors.toList());
    }
    public List<IDeleteOperation> getDeleteOperations() {
        return operations.stream()
                .filter(o -> o instanceof IDeleteOperation)
                .map(o -> (IDeleteOperation) o)
                .collect(Collectors.toList());
    }

    public long getAddCount() {
        return addCount;
    }

    public long getRemoveCount() {
        return removeCount;
    }

    @Override
    public synchronized void add(
            String reference, InputStream content, Properties metadata) {
        LOG.debug("Committing addition of {}", reference);
        operations.add(new MemoryAddOperation(reference, content, metadata));
        addCount++;
    }

    @Override
    public synchronized void remove(String reference, Properties metadata) {
        LOG.debug("Committing removal of {}", reference);
        operations.add(new IDeleteOperation() {
            private static final long serialVersionUID = 1L;
            @Override
            public void delete() {
                operations.remove(this);
            }
            @Override
            public String getReference() {
                return reference;
            }
        });
        removeCount++;
    }

    @Override
    public void commit() {
        LOG.info("{} additions committed.", addCount);
        LOG.info("{} deletions committed.", removeCount);
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

    private class MemoryAddOperation implements IAddOperation {
        private static final long serialVersionUID = 1L;
        private final transient ByteArrayInputStream content;
        private final Properties metadata = new Properties();
        private final String reference;
        public MemoryAddOperation(
                String reference, InputStream content, Properties metadata) {
            super();
            this.reference = reference;
            if (content == null || isIgnoreContent()) {
                this.content = null;
            } else {
                try {
                    this.content = new ByteArrayInputStream(
                            IOUtils.toByteArray(content));
                } catch (IOException e) {
                    throw new CommitterException(
                            "Could not commit addition of " + reference);
                }
            }

            if (metadata != null) {
                if (StringUtils.isBlank(getFieldsRegex())) {
                    this.metadata.loadFromMap(metadata);
                } else {
                    this.metadata.loadFromMap(metadata.entrySet().stream()
                            .filter(en -> en.getKey().matches(fieldsRegex))
                            .collect(Collectors.toMap(
                                    Entry::getKey, Entry::getValue)));
                }
            }
        }

        @Override
        public void delete() {
            operations.remove(this);
        }
        @Override
        public InputStream getContentStream() throws IOException {
            return content;
        }
        @Override
        public Properties getMetadata() {
            return metadata;
        }
        @Override
        public String getReference() {
            return reference;
        }
    }
}
