/* Copyright 2018-2020 Norconex Inc.
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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.norconex.commons.lang.event.Event;

/**
 * Default committer events.
 * @author Pascal Essiembre
 * @param <T> Committer for this event
 * @since 3.0.0
 */
public class CommitterEvent<T extends ICommitter> extends Event<T> {

    private static final long serialVersionUID = 1L;

    /** The Committer began its initialization. */
    public static final String COMMITTER_INIT_BEGIN = "COMMITTER_INIT_BEGIN";
    /** The Committer has been initialized. */
    public static final String COMMITTER_INIT_END = "COMMITTER_INIT_END";
    /** The Committer encountered an error when initializing. */
    public static final String COMMITTER_INIT_ERROR = "COMMITTER_INIT_ERROR";

    /** The Committer is receiving a document to be updated or inserted. */
    public static final String COMMITTER_UPSERT_BEGIN =
            "COMMITTER_UPSERT_BEGIN";
    /** The Committer has received a document to be updated or inserted.*/
    public static final String COMMITTER_UPSERT_END = "COMMITTER_UPSERT_END";
    /** The Committer entity update/upsert produced an error. */
    public static final String COMMITTER_UPSERT_ERROR =
            "COMMITTER_UPSERT_ERROR";

    /** The Committer is receiving document to be removed. */
    public static final String COMMITTER_DELETE_BEGIN =
            "COMMITTER_DELETE_BEGIN";
    /** The Committer has received a document to be removed. */
    public static final String COMMITTER_DELETE_END = "COMMITTER_DELETE_END";
    /** The Committer entity removal produced an error. */
    public static final String COMMITTER_DELETE_ERROR =
            "COMMITTER_DELETE_ERROR";

    /**
     * The Committer is about to commit a request batch.
     * Triggered by supporting Committers only.
     */
    public static final String COMMITTER_BATCH_BEGIN = "COMMITTER_BATCH_BEGIN";
    /**
     * The Committer is done committing a request batch
     * Triggered by supporting Committers only.
     */
    public static final String COMMITTER_BATCH_END = "COMMITTER_BATCH_END";
    /**
     * The Committer encountered an error when committing a request batch.
     * Triggered by supporting Committers only.
     */
    public static final String COMMITTER_BATCH_ERROR = "COMMITTER_BATCH_ERROR";

    /** The Committer is closing. */
    public static final String COMMITTER_CLOSE_BEGIN = "COMMITTER_CLOSE_BEGIN";
    /** The Committer is closed. */
    public static final String COMMITTER_CLOSE_END = "COMMITTER_CLOSE_END";
    /** The Committer encountered an error when closing. */
    public static final String COMMITTER_CLOSE_ERROR = "COMMITTER_CLOSE_ERROR";

    //TODO add a constant for "commit_error" and maybe "commit_warning"?


    private final ICommitterRequest request;

    /**
     * New crawler event.
     * @param name event name
     * @param committer Committer responsible for triggering the event
     * @param request the entity being upserted/deleted
     * @param exception exception tied to this event (may be <code>null</code>)
     */
    public CommitterEvent(String name, T committer,
            ICommitterRequest request, Throwable exception) {
        super(name, committer, exception);
        this.request = request;
    }

    public static CommitterEvent<ICommitter> create(
            String name, ICommitter committer) {
        return create(name, committer, (ICommitterRequest) null);
    }
    public static CommitterEvent<ICommitter> create(
            String name, ICommitter committer, ICommitterRequest request) {
        return new CommitterEvent<>(name, committer, request, null);
    }
    public static CommitterEvent<ICommitter> create(
            String name, ICommitter committer, Throwable exception) {
        return new CommitterEvent<>(name, committer, null, exception);
    }
    public static CommitterEvent<ICommitter> create(
            String name, ICommitter committer,
            ICommitterRequest request, Throwable exception) {
        return new CommitterEvent<>(name, committer, request, exception);
    }

    public ICommitterRequest getRequest() {
        return request;
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
