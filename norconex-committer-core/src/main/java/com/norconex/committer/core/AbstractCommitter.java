/* Copyright 2010-2018 Norconex Inc.
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

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.xml.IXMLConfigurable;

/**
 * <p>
 * Basic implementation invoking the {@link #commit()} method every time a given
 * queue size threshold has been reached.  Both additions and deletions count
 * towards the same queue size.
 * It is left to implementors to decide how to actually queue the
 * documents and how to perform commits.
 * </p>
 * <p>
 * Consider extending {@link AbstractFileQueueCommitter} if you do not wish
 * to implement your own queue.
 * </p>
 * <p>Subclasses implementing {@link IXMLConfigurable} should allow this inner
 * configuration:</p>
 * <pre>
 *      &lt;queueSize&gt;(max queue size before committing)&lt;/queueSize&gt;
 * </pre>
 *
 * @author Pascal Essiembre
 * @since 1.1.0
 */
public abstract class AbstractCommitter implements ICommitter {

    private static final Logger LOG = LoggerFactory.getLogger(
            AbstractCommitter.class);

    /** Default queue size. */
    public static final int DEFAULT_QUEUE_SIZE = 1000;

    protected int queueSize = DEFAULT_QUEUE_SIZE;
    private AtomicLong docCount = null;

    /**
     * Constructor.
     */
    public AbstractCommitter() {
        super();
    }
    /**
     * Constructor.
     * @param queueSize queue size
     */
    public AbstractCommitter(int queueSize) {
        super();
        this.queueSize = queueSize;
    }

    /**
     * Gets the queue size.
     * @return queue size
     */
    public int getQueueSize() {
        return queueSize;
    }
    /**
     * Sets the queue size.
     * @param queueSize queue size
     */
    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    @Override
    public final void add(
            String reference, InputStream content, Properties metadata) {
        ensureInitialDocCount();
        queueAddition(reference, content, metadata);
        commitIfReady();
    }
    /**
     * Queues a document to be added.
     * @param reference document reference
     * @param content document content
     * @param metadata document metadata
     */
    protected abstract void queueAddition(
            String reference, InputStream content, Properties metadata);

    @Override
    public final void remove(
            String reference, Properties metadata) {
        ensureInitialDocCount();
        queueRemoval(reference, metadata);
        commitIfReady();
    }

    /**
     * Gets the initial document count, in case there are already documents
     * in the queue the first time the committer is used.  Otherwise,
     * returns zero.
     * @return zero or the initial number of documents in the queue
     */
    protected abstract long getInitialQueueDocCount();

    /**
     * Queues a document to be deleted.
     * @param reference document reference
     * @param metadata document metadata
     */
    protected abstract void queueRemoval(
            String reference, Properties metadata);

    private synchronized void ensureInitialDocCount() {
        if (docCount == null) {
            docCount = new AtomicLong(getInitialQueueDocCount());
        }
    }

    @SuppressWarnings("nls")
    private void commitIfReady() {
        long count = docCount.incrementAndGet();
        if (queueSize == 0 || count % queueSize == 0) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Max queue size reached (" + queueSize
                        + "). Committing");
            }
            commit();
        }
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