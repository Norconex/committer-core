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

import java.io.File;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.norconex.commons.lang.TimeIdGenerator;
import com.norconex.commons.lang.event.EventManager;
import com.norconex.commons.lang.io.CachedStreamFactory;

/**
 * Holds data defined outside a committer but useful or required for the
 * committer execution.
 * 
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public final class CommitterContext {

    private EventManager eventManager;
    private Path workDir;
    private CachedStreamFactory streamFactory;

    private CommitterContext() {
        super();
    }

    /**
     * Gets the event manager used to fire committer events.
     * 
     * @return event manager
     */
    public EventManager getEventManager() {
        return eventManager;
    }

    /**
     * Gets a unique working directory for a committer (if one is needed).
     * 
     * @return working directory (never <code>null</code>)
     */
    public Path getWorkDir() {
        return workDir;
    }

    /**
     * Gets the cached stream factory used by committers.
     * 
     * @return cached stream factory
     */
    public CachedStreamFactory getStreamFactory() {
        return streamFactory;
    }

    /**
     * Creates a copy of this context with a different event manager.
     * 
     * @param eventManager event manager to use
     * @return new context instance
     */
    public CommitterContext withEventManager(EventManager eventManager) {
        return CommitterContext.builder()
                .setEventManager(eventManager)
                .setWorkDir(workDir)
                .setStreamFactory(streamFactory)
                .build();
    }

    /**
     * Creates a copy of this context with a different working directory.
     * 
     * @param workDir working directory to use
     * @return new context instance
     */
    public CommitterContext withWorkdir(Path workDir) {
        return CommitterContext.builder()
                .setEventManager(eventManager)
                .setWorkDir(workDir)
                .setStreamFactory(streamFactory)
                .build();
    }

    /**
     * Creates a copy of this context with a different stream factory.
     * 
     * @param streamFactory stream factory to use
     * @return new context instance
     */
    public CommitterContext withStreamFactory(
            CachedStreamFactory streamFactory) {
        return CommitterContext.builder()
                .setEventManager(eventManager)
                .setWorkDir(workDir)
                .setStreamFactory(streamFactory)
                .build();
    }

    /**
     * Creates a builder for {@link CommitterContext}.
     * 
     * @return context builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link CommitterContext}.
     */
    public static class Builder {
        private final CommitterContext ctx = new CommitterContext();

        private Builder() {
            super();
        }

        /**
         * Sets the context working directory.
         * 
         * @param workDir working directory
         * @return this builder
         */
        public Builder setWorkDir(Path workDir) {
            ctx.workDir = workDir;
            return this;
        }

        /**
         * Sets the context event manager.
         * 
         * @param eventManager event manager
         * @return this builder
         */
        public Builder setEventManager(EventManager eventManager) {
            ctx.eventManager = eventManager;
            return this;
        }

        /**
         * Sets the cached stream factory.
         * 
         * @param streamFactory stream factory
         * @return this builder
         */
        public Builder setStreamFactory(CachedStreamFactory streamFactory) {
            ctx.streamFactory = streamFactory;
            return this;
        }

        /**
         * Builds a context and applies defaults where needed.
         * 
         * @return a fully initialized context
         */
        public CommitterContext build() {
            if (ctx.workDir == null) {
                ctx.workDir = new File(FileUtils.getTempDirectory(),
                        "committer-" + TimeIdGenerator.next()).toPath();
            }
            if (ctx.eventManager == null) {
                ctx.eventManager = new EventManager();
            }
            if (ctx.streamFactory == null) {
                ctx.streamFactory = new CachedStreamFactory();
            }
            return ctx;
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
