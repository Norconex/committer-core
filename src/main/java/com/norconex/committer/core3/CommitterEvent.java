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
 * @since 3.0.0
 */
public class CommitterEvent extends Event {

    private static final long serialVersionUID = 1L;

    /** The Committer began its initialization. */
    public static final String COMMITTER_INIT_BEGIN = "COMMITTER_INIT_BEGIN";
    /** The Committer has been initialized. */
    public static final String COMMITTER_INIT_END = "COMMITTER_INIT_END";
    /** The Committer encountered an error when initializing. */
    public static final String COMMITTER_INIT_ERROR = "COMMITTER_INIT_ERROR";

    /** The Committer has accepted a request and it will commit it. */
    public static final String COMMITTER_ACCEPT_YES = "COMMITTER_ACCEPT_YES";
    /** The Committer has rejected a request and it will not commit it. */
    public static final String COMMITTER_ACCEPT_NO = "COMMITTER_ACCEPT_NO";
    /** The Committer acceptance check produced an error. */
    public static final String COMMITTER_ACCEPT_ERROR =
            "COMMITTER_ACCEPT_ERROR";

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

    /** The Committer is being cleaned. */
    public static final String COMMITTER_CLEAN_BEGIN = "COMMITTER_CLEAN_BEGIN";
    /** The Committer has been cleaned. */
    public static final String COMMITTER_CLEAN_END = "COMMITTER_CLEAN_END";
    /** The Committer encountered an error when cleaning. */
    public static final String COMMITTER_CLEAN_ERROR = "COMMITTER_CLEAN_ERROR";


    //TODO add a constant for "commit_error" and maybe "commit_warning"?


    private final ICommitterRequest request;

    public static class Builder extends Event.Builder<Builder> {

        private ICommitterRequest request;

        public Builder(String name, ICommitter source) {
            super(name, source);
        }

        public Builder committerRequest(ICommitterRequest request) {
            this.request = request;
            return this;
        }

        @Override
        public CommitterEvent build() {
            return new CommitterEvent(this);
        }
    }

    /**
     * New event.
     * @param b builder
     */
    protected CommitterEvent(Builder b) {
        super(b);
        this.request = b.request;
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
        ReflectionToStringBuilder b = new ReflectionToStringBuilder(
                this, ToStringStyle.SHORT_PREFIX_STYLE);
        b.setExcludeNullValues(true);
        return b.toString();
    }
}
