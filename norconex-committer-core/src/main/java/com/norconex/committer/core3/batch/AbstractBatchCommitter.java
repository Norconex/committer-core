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
package com.norconex.committer.core3.batch;

import java.util.Iterator;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.norconex.committer.core3.AbstractCommitter;
import com.norconex.committer.core3.CommitterEvent;
import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.ICommitterRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.committer.core3.batch.queue.ICommitterQueue;
import com.norconex.committer.core3.batch.queue.impl.FSQueue;
import com.norconex.commons.lang.xml.IXMLConfigurable;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>
 * A base implementation for doing batch commits. Uses an internal queue
 * for storing update/addition requests and deletion requests.
 * It sends the queued data to the remote target every time a given
 * queue threshold has been reached.  Unless otherwise stated,
 * both additions and deletions count towards that threshold.
 * </p>
 * <p>
 * This class also provides batch-related events:
 * <code>COMMITTER_BATCH_BEGIN</code>,
 * <code>COMMITTER_BATCH_END</code>, and
 * <code>COMMITTER_BATCH_ERROR</code>.
 * </p>
 *
 * <p>
 * The default queue is {@link FSQueue} (file-system queue).
 * </p>
 *
 * <p>Subclasses inherits this {@link IXMLConfigurable} configuration:</p>
 *
 * {@nx.xml #options
 *   <!-- Default queue settings ("class" is optional): -->
 *   {@nx.include com.norconex.committer.core3.batch.queue.impl.FSQueue@@nx.xml.usage}
 *
 *   {@nx.include com.norconex.committer.core3.AbstractCommitter#options}
 * }
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
@SuppressWarnings("javadoc")
public abstract class AbstractBatchCommitter extends AbstractCommitter
        implements IXMLConfigurable, IBatchConsumer {

//    private static final Logger LOG =
//            LoggerFactory.getLogger(AbstractBatchCommitter.class);

    private ICommitterQueue queue;

    //TODO add support for these?
//    private int maxRetries;
//    private long maxRetryWait;



    @Override
    protected final void doInit() throws CommitterException {
        if (this.queue == null) {
            this.queue = new FSQueue();
        }
        initBatchCommitter();
        this.queue.init(getCommitterContext(), this);
    }
    @Override
    protected void doUpsert(UpsertRequest upsertRequest)
            throws CommitterException {
        queue.queue(upsertRequest);
    }
    @Override
    protected void doDelete(DeleteRequest deleteRequest)
            throws CommitterException {
        queue.queue(deleteRequest);
    }
    @Override
    protected void doClose() throws CommitterException {
        try {
            closeBatchCommitter();
        } finally {
            this.queue.close();
        }
    }
    @Override
    protected void doClean() throws CommitterException {
        this.queue.clean();
    }

    @Override
    public void consume(Iterator<ICommitterRequest> it)
            throws CommitterException {
        fireInfo(CommitterEvent.COMMITTER_BATCH_BEGIN);
        try {
            commitBatch(it);
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_BATCH_ERROR, e);
            throw  e;
        }
        fireInfo(CommitterEvent.COMMITTER_BATCH_END);
    }

    @Override
    public final void loadCommitterFromXML(XML xml) {
        loadBatchCommitterFromXML(xml);
        setCommitterQueue(
                xml.getObjectImpl(ICommitterQueue.class, "queue", queue));
    }
    @Override
    public final void saveCommitterToXML(XML xml) {
        saveBatchCommitterToXML(xml);
        xml.addElement("queue", queue);
    }

    protected abstract void loadBatchCommitterFromXML(XML xml);
    protected abstract void saveBatchCommitterToXML(XML xml);

    public ICommitterQueue getCommitterQueue() {
        return this.queue;
    }
    public void setCommitterQueue(ICommitterQueue queue) {
        this.queue = queue;
    }

    /**
     * Subclasses can perform additional initialization by overriding this
     * method. Default implementation does nothing. The committer context
     * and committer queue will be already initialized when invoking
     * {@link #getCommitterContext()} and {@link #getCommitterQueue()},
     * respectively.
     * @throws CommitterException error initializing
     */
    protected void initBatchCommitter()
            throws CommitterException {
        //NOOP
    }
    protected abstract void commitBatch(Iterator<ICommitterRequest> it)
            throws CommitterException;

    /**
     * Subclasses can perform additional closing logic by overriding this
     * method. Default implementation does nothing.
     * @throws CommitterException error closing committer
     */
    protected void closeBatchCommitter() throws CommitterException {
        //NOOP
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