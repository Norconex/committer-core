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
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with Norconex Committer. If not, see <http://www.gnu.org/licenses/>.
*/
package com.norconex.committer;

import java.io.File;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.commons.lang.map.Properties;

/**
* Base implementation offering to batch the committing of documents
* (additions and deletions alike).
* @author Pascal Essiembre
* @deprecated Since 1.1.0. Use {@link AbstractCommitter}
*/
@Deprecated
public abstract class BatchableCommitter implements ICommitter {

    private static final long serialVersionUID = 880638478926236689L;
    private static final Logger LOG = LogManager.getLogger(
            BatchableCommitter.class);
    
    /** Default batch size. */
    public static final int DEFAULT_BATCH_SIZE = 1000;
    
    private int batchSize = DEFAULT_BATCH_SIZE;
    private long docCount;
    
    /**
     * Constructor.
     */
    public BatchableCommitter() {
        super();
    }
    /**
     * Creates a new instance initialized to the given batch size.
     * @param batchSize batch size
     */
    public BatchableCommitter(int batchSize) {
        super();
        this.batchSize = batchSize;
    }
    
    /**
     * Gets the batch size.
     * @return batch size
     */
    public int getBatchSize() {
        return batchSize;
    }
    /**
     * Sets the batch size.
     * @param batchSize batch size
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public final void queueAdd(
            String reference, File document, Properties metadata) {
        queueBatchableAdd(reference, document, metadata);
        commitIfReady();
    }
    /**
     * Queues a document to be added.
     * @param reference document reference
     * @param document document file
     * @param metadata document metadata
     */
    protected abstract void queueBatchableAdd(
            String reference, File document, Properties metadata);

    @Override
    public final void queueRemove(
            String ref, File document, Properties metadata) {
        queueBatchableRemove(ref, document, metadata);
        commitIfReady();
    }
    /**
     * Queues a document to be deleted.
     * @param reference document reference
     * @param document document file
     * @param metadata document metadata
     */
    protected abstract void queueBatchableRemove(
            String reference, File document, Properties metadata);
    

    @SuppressWarnings("nls")
    private void commitIfReady() {
        docCount++;
        if (docCount % batchSize == 0) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Batch size reached (" + batchSize
                        + "). Committing");
            }
            commit();
        }
    }
    
    @Override
    public int hashCode() {
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder();
        hashCodeBuilder.append(batchSize);
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
        if (!(obj instanceof BatchableCommitter)) {
            return false;
        }
        BatchableCommitter other = (BatchableCommitter) obj;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(batchSize, other.batchSize);
        equalsBuilder.append(docCount, other.docCount);
        return equalsBuilder.isEquals();
    }
    
    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append("batchSize", batchSize);
        builder.append("docCount", docCount);
        return builder.toString();
    }
}