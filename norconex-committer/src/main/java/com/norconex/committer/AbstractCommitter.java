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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Norconex Committer. If not, see <http://www.gnu.org/licenses/>.
 */
package com.norconex.committer;

import java.io.InputStream;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.map.Properties;

/**
 * Basic implementation invoking the {@link #commit()} method every time a given
 * queue size threshold has been reached.  Both additions and deletions count
 * towards the same queue size.
 * It is left to implementors to decide how to actually queue the 
 * documents and how to perform commits.
 * <p />
 * Consider extending {@link AbstractFileQueueCommitter} if you do not wish
 * to implement your own queue.
 * 
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

    private static final long serialVersionUID = 880638478926236689L;
    private static final Logger LOG = LogManager.getLogger(
            AbstractCommitter.class);
    
    /** Default queue size. */
    public static final int DEFAULT_QUEUE_SIZE = 1000;
    
    private int queueSize = DEFAULT_QUEUE_SIZE;
    private long docCount;
    
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
        queueRemoval(reference, metadata);
        commitIfReady();
    }
    /**
     * Queues a document to be deleted.
     * @param reference document reference
     * @param content document content
     * @param metadata document metadata
     */
    protected abstract void queueRemoval(
            String reference, Properties metadata);

    @SuppressWarnings("nls")
    private void commitIfReady() {
        docCount++;
        if (docCount % queueSize == 0) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Batch size reached (" + queueSize
                        + "). Committing");
            }
            commit();
        }
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder();
        hashCodeBuilder.append(queueSize);
        hashCodeBuilder.append(docCount);
        return hashCodeBuilder.toHashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof AbstractCommitter)) {
            return false;
        }
        AbstractCommitter other = (AbstractCommitter) obj;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(queueSize, other.queueSize);
        equalsBuilder.append(docCount, other.docCount);
        return equalsBuilder.isEquals();
    }
    
    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append("queueSize", queueSize);
        builder.append("docCount", docCount);
        return builder.toString();
    }
}