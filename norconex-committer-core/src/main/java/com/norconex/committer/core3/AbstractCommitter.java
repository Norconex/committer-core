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
package com.norconex.committer.core3;

import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.event.Level;

/**
 * <p>
 * A base implementation taking care of basic plumbing, such as
 * firing main Committer events (including exceptions) and
 * storing the Committer context (available via {@link #getCommitterContext()}).
 * </p>
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public abstract class AbstractCommitter implements ICommitter {

    private CommitterContext committerContext;

    @Override
    public final void init(
            CommitterContext committerContext) throws CommitterException {
        this.committerContext = Objects.requireNonNull(
                committerContext, "'committerContext' must not be null.");
        fireInfo(CommitterEvent.COMMITTER_INIT_BEGIN);
        try {
            doInit();
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_INIT_ERROR, e);
            throw e;
        }
        fireInfo(CommitterEvent.COMMITTER_INIT_END);
    }

    @Override
    public final void upsert(
            UpsertRequest upsertRequest) throws CommitterException {
        fireInfo(CommitterEvent.COMMITTER_UPSERT_BEGIN, upsertRequest);
        try {
            doUpsert(upsertRequest);
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
            doDelete(deleteRequest);
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_DELETE_ERROR, deleteRequest, e);
            throw e;
        }
        fireInfo(CommitterEvent.COMMITTER_DELETE_END, deleteRequest);
    }

    @Override
    public final void close() throws CommitterException {
        fireInfo(CommitterEvent.COMMITTER_CLOSE_BEGIN);
        try {
            doClose();
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_CLOSE_ERROR, e);
            throw e;
        }
        fireInfo(CommitterEvent.COMMITTER_CLOSE_END);
    }

    protected CommitterContext getCommitterContext() {
        return this.committerContext;
    }

    /**
     * Subclasses can perform additional initialization by overriding this
     * method. Default implementation does nothing. The
     * {@link CommitterContext} will be initialized when invoking
     * {@link #getCommitterContext()}
     * @throws CommitterException error initializing
     */
    protected abstract void doInit()
            throws CommitterException;
    protected abstract void doUpsert(UpsertRequest upsertRequest)
            throws CommitterException;
    protected abstract void doDelete(DeleteRequest deleteRequest)
            throws CommitterException;
    /**
     * Subclasses can perform additional closing logic by overriding this
     * method. Default implementation does nothing.
     * @throws CommitterException error closing committer
     */
    protected abstract void doClose() throws CommitterException;

    protected final void fireDebug(String name) {
        fireInfo(name, null);
    }
    protected final void fireDebug(String name, ICommitterRequest req) {
        committerContext.getEventManager().fire(
                CommitterEvent.create(name, this, req), Level.DEBUG);
    }
    protected final void fireInfo(String name) {
        fireInfo(name, null);
    }
    protected final void fireInfo(String name, ICommitterRequest req) {
        committerContext.getEventManager().fire(
                CommitterEvent.create(name, this, req));
    }
    protected final void fireError(String name, Exception e) {
        fireError(name, null, e);
    }
    protected final void fireError(
            String name, ICommitterRequest req, Exception e) {
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