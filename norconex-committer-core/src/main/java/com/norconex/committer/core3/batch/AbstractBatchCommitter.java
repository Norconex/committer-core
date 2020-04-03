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
import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.event.Level;

import com.norconex.committer.core3.CommitterContext;
import com.norconex.committer.core3.CommitterEvent;
import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.ICommitter;
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
 * queue size threshold has been reached.  Both additions and deletions count
 * towards the same queue size.
 * </p>
 * <p>
 * The default queue is {@link FSQueue} (file-system queue).
 * </p>
 *
 * <p>Subclasses inherits this {@link IXMLConfigurable} configuration:</p>
 *
 * {@nx.xml #options
 *   <queue class="(optional custom queue implementation class)">
 *     (custom queue configuration options, if applicable)
 *   </queue>
 * }
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public abstract class AbstractBatchCommitter
        implements ICommitter, IXMLConfigurable, BatchConsumer {

    private CommitterContext committerContext;
    private ICommitterQueue queue;

    @Override
    public final void init(
            CommitterContext committerContext) throws CommitterException {
        this.committerContext = Objects.requireNonNull(
                committerContext, "'committerContext' must not be null.");
        fireInfo(CommitterEvent.COMMITTER_INIT_BEGIN, null);

        try {
            if (this.queue == null) {
                this.queue = new FSQueue();
            }
            this.queue.init(committerContext, this);

            initCommitter(committerContext);
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_INIT_ERROR, null, e);
            throw e;
        }
        fireInfo(CommitterEvent.COMMITTER_INIT_END, null);
    }

    public ICommitterQueue getQueue() {
        return queue;
    }
    public void setQueue(ICommitterQueue queue) {
        this.queue = queue;
    }

    @Override
    public final void upsert(
            UpsertRequest upsertRequest) throws CommitterException {
        fireInfo(CommitterEvent.COMMITTER_UPSERT_BEGIN, upsertRequest);
        try {
            queue.queue(upsertRequest);
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_UPSERT_ERROR, upsertRequest, e);
            throw e;
        }
        fireInfo(CommitterEvent.COMMITTER_UPSERT_END, upsertRequest);
    }
    @Override
    public final void delete(
            DeleteRequest deleteRequest) throws CommitterException {
        fireInfo(CommitterEvent.COMMITTER_DELETE_BEGIN, deleteRequest);
        try {
            queue.queue(deleteRequest);
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_DELETE_ERROR, deleteRequest, e);
            throw e;
        }
        fireInfo(CommitterEvent.COMMITTER_DELETE_END, deleteRequest);
    }

    @Override
    public void consume(Iterator<ICommitterRequest> it)
            throws CommitterException {
        fireInfo(CommitterEvent.COMMITTER_BATCH_BEGIN, null);
        try {
            commitBatch(it);
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_BATCH_ERROR, null, e);
            throw  e;
        }
        fireInfo(CommitterEvent.COMMITTER_BATCH_END, null);
    }

    @Override
    public final void close() throws CommitterException {
        fireInfo(CommitterEvent.COMMITTER_CLOSE_BEGIN, null);
        try {
            this.queue.close();
            closeCommitter();
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_CLOSE_ERROR, null, e);
            throw e;
        }
        fireInfo(CommitterEvent.COMMITTER_CLOSE_END, null);
    }


    @Override
    public final void loadFromXML(XML xml) {
        loadBatchCommitterFromXML(xml);
        setQueue(xml.getObjectImpl(ICommitterQueue.class, "queue", queue));
    }
    @Override
    public final void saveToXML(XML xml) {
        saveBatchCommitterToXML(xml);
        xml.addElement("queue", queue);
    }

    protected abstract void loadBatchCommitterFromXML(XML xml);
    protected abstract void saveBatchCommitterToXML(XML xml);

    protected CommitterContext getCommitterContext() {
        return this.committerContext;
    }
    protected ICommitterQueue getCommitterQueue() {
        return this.queue;
    }

    /**
     * Subclasses can perform additional initialization by overriding this
     * method. Default implementation does nothing.
     * @param committerContext contextual data helping with initialization
     * @throws CommitterException error initializing
     */
    protected void initCommitter(CommitterContext committerContext)
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
    protected void closeCommitter() throws CommitterException {
        //NOOP
    }

    private void fireInfo(String name, ICommitterRequest req) {
        committerContext.getEventManager().fire(
                CommitterEvent.create(name, this, req));
    }
    private void fireError(String name, ICommitterRequest req, Exception e) {
        committerContext.getEventManager().fire(
                CommitterEvent.create(name, this, req, e), Level.ERROR);
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