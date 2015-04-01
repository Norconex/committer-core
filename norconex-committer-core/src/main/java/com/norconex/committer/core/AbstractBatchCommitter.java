/* Copyright 2010-2014 Norconex Inc.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.commons.lang.Sleeper;
import com.norconex.commons.lang.config.IXMLConfigurable;


/**
 * Commits documents to the target repository
 * (e.g. search engine) in batch.  That is, multiple documents are expected
 * to be sent in one requests to the target repository.  To achieve this,
 * operations are cached in memory until the commit batch size is reached, then
 * the operations cached so far are sent.
 * <br><br>
 * After being committed, the documents are automatically removed from the 
 * queue.
 * <br><br>
 * If you need to map original document fields with target repository fields,
 * consider using {@link AbstractMappedCommitter}.
 * 
 * <p>Subclasses implementing {@link IXMLConfigurable} should allow this inner 
 * configuration:</p>
 * <pre>
 *      &lt;commitBatchSize&gt;
 *          (max number of documents to send IDOL at once)
 *      &lt;/commitBatchSize&gt;
 *      &lt;queueDir&gt;(optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueSize&gt;(max queue size before committing)&lt;/queueSize&gt;
 *      &lt;maxRetries&gt;(max retries upon commit failures)&lt;/maxRetries&gt;
 *      &lt;maxRetryWait&gt;(max delay between retries)&lt;/maxRetryWait&gt;
 * </pre>
 * 
 * @author Pascal Essiembre
 * @since 1.1.0
 */
public abstract class AbstractBatchCommitter 
        extends AbstractFileQueueCommitter {

    private static final Logger LOG = LogManager.getLogger(
            AbstractBatchCommitter.class);
    
    /** Default commit batch size. */
    public static final int DEFAULT_COMMIT_BATCH_SIZE = 100;

    private int commitBatchSize;
    private int maxRetries;
    private long maxRetryWait;

    private final Set<ICommitOperation> operations = 
            Collections.synchronizedSet(new ListOrderedSet<ICommitOperation>());

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

    /**
     * Gets the maximum number of retries upon batch commit failure.
     * Default is zero (does not retry).
     * @return maximum number of retries
     * @since 1.2.0
     */
    public int getMaxRetries() {
        return maxRetries;
    }
    /**
     * Sets the maximum number of retries upon batch commit failure.
     * @param maxRetries maximum number of retries
     * @since 1.2.0
     */
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
    
    /**
     * Gets the maximum wait time before retrying a failed commit.
     * @return maximum wait time 
     * @since 1.2.0
     */
    public long getMaxRetryWait() {
        return maxRetryWait;
    }
    /**
     * Sets the maximum wait time before retrying a failed commit.
     * @param maxRetryWait maximum wait time 
     * @since 1.2.0
     */
    public void setMaxRetryWait(long maxRetryWait) {
        this.maxRetryWait = maxRetryWait;
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
        int numTries = 0;
        boolean success = false;
        while (!success) {
            try {
                commitBatch(batch);
                success = true;
            } catch (Exception e) {
                if (numTries < maxRetries) {
                    LOG.error("Could not commit batched operations.", e);
                    Sleeper.sleepMillis(maxRetryWait);
                    numTries++;
                } else {
                    throw (RuntimeException) e;
                }
            }
        }
        
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
        hashCodeBuilder.append(maxRetries);
        hashCodeBuilder.append(maxRetryWait);
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
        equalsBuilder.append(maxRetries, other.maxRetries);
        equalsBuilder.append(maxRetryWait, other.maxRetryWait);
        equalsBuilder.append(operations, other.operations);
        return equalsBuilder.isEquals();
    }
    
    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.appendSuper(super.toString());
        builder.append("commitBatchSize", commitBatchSize);
        builder.append("maxRetries", maxRetries);
        builder.append("maxRetryWait", maxRetryWait);
        builder.append("operations", operations);
        return builder.toString();
    }
}
