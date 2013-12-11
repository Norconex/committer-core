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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;


/**
 * Commits documents to the target repository
 * (e.g. search engine) in batch.  That is, multiple documents are expected
 * to be sent in one requests to the target repository.  To achieve this,
 * operations are cached in memory until the commit batch size is reached, then
 * the operations cached so far are sent.
 * <p/>
 * After being committed, the documents are automatically removed from the 
 * queue.
 * <p/>
 * If you need to map original document fields with target repository fields,
 * consider using {@link AbstractMappedCommitter}.
 * 
 * @author Pascal Essiembre
 * @since 1.1.0
 */
public abstract class AbstractBatchCommitter 
        extends AbstractFileQueueCommitter {

    private static final long serialVersionUID = 9162884038430884000L;

    /** Default commit batch size. */
    public static final int DEFAULT_COMMIT_BATCH_SIZE = 100;

    private int commitBatchSize;

    private final List<ICommitOperation> operations = 
            Collections.synchronizedList(new ArrayList<ICommitOperation>());

    /**
     * Constructor.
     */
    public AbstractBatchCommitter() {
        this(DEFAULT_COMMIT_BATCH_SIZE);
    }
    /**
     * Constructor.
     * @param commitBatchSize commit batch size
     */
    public AbstractBatchCommitter(int commitBatchSize) {
        super();
        this.commitBatchSize = commitBatchSize;
    }
    /**
     * Gets the commit batch size.
     * @return commit batch size
     */
    public int getCommitBatchSize() {
        return commitBatchSize;
    }
    /**
     * Sets the commit batch size.
     * @param commitBatchSize commit batch size
     */
    public void setCommitBatchSize(int commitBatchSize) {
        this.commitBatchSize = commitBatchSize;
    }

    @Override
    protected final void commitAddition(IAddOperation operation) {
        cacheOperationAndCommitIfReady(operation);
    }

    @Override
    protected final void commitDeletion(IDeleteOperation operation) {
        cacheOperationAndCommitIfReady(operation);
    }

    @Override
    protected void commitComplete() {
        if (!operations.isEmpty()) {
            List<ICommitOperation> batch = null;
            synchronized (operations) {
                batch = getBatchToCommit();
            }
            if (batch != null) {
                commitAndCleanBatch(batch);
            }
        }
    }

    /**
     * Commits a group of operation.
     * @param batch the group of operations
     */
    protected abstract void commitBatch(List<ICommitOperation> batch);
   
    /**
     * Commits documents and delete them from queue when done.
     * @param batch the bath of operations to commit.
     */
    private void commitAndCleanBatch(List<ICommitOperation> batch) {
        commitBatch(batch);
        // Delete queued documents after commit
        for (ICommitOperation op : batch) {
            op.delete();
        }
        batch.clear();
    }
    
    private void cacheOperationAndCommitIfReady(ICommitOperation operation) {
        operations.add(operation);
        List<ICommitOperation> batch = null;
        synchronized (operations) {
            if (operations.size() % commitBatchSize == 0) {
                batch = getBatchToCommit();
            }
        }
        if (batch != null) {
            commitAndCleanBatch(batch);
        }
    }
    
    private List<ICommitOperation> getBatchToCommit() {
        List<ICommitOperation> batch = 
                new ArrayList<ICommitOperation>(operations);
        operations.clear();
        return batch;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder();
        hashCodeBuilder.append(commitBatchSize);
        hashCodeBuilder.append(operations);
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
        if (!(obj instanceof AbstractBatchCommitter)) {
            return false;
        }
        AbstractBatchCommitter other = (AbstractBatchCommitter) obj;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(commitBatchSize, other.commitBatchSize);
        equalsBuilder.append(operations, other.operations);
        return equalsBuilder.isEquals();
    }
    
    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.appendSuper(super.toString());
        builder.append("commitBatchSize", commitBatchSize);
        builder.append("operations", operations);
        return builder.toString();
    }
}
