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

/**
 * Holds data defined outside a committer but useful or required for the
 * committer execution.
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public final class CommitterContext {

    private EventManager eventManager;
    private Path workDir;

    private CommitterContext() {
        super();
    }

    public EventManager getEventManager() {
        return eventManager;
    }
    /**
     * Gets a unique working directory for a committer (if one is needed).
     * @return working directory (never <code>null</code>)
     */
    public Path getWorkDir() {
        return workDir;
    }
    public CommitterContext withEventManager(EventManager eventManager) {
        return CommitterContext.build()
                .setEventManager(eventManager)
                .setWorkDir(workDir).create();
    }
    public CommitterContext withWorkdir(Path workDir) {
        return CommitterContext.build()
                .setEventManager(eventManager)
                .setWorkDir(workDir).create();
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private final CommitterContext ctx = new CommitterContext();
        public Builder setWorkDir(Path workDir) {
            ctx.workDir = workDir;
            return this;
        }
        public Builder setEventManager(EventManager eventManager) {
            ctx.eventManager = eventManager;
            return this;
        }
        public CommitterContext create() {
            if (ctx.workDir == null) {
                ctx.workDir = new File(FileUtils.getTempDirectory(),
                        "committer-" + TimeIdGenerator.next()).toPath();
            }
            if (ctx.eventManager == null) {
                ctx.eventManager = new EventManager();
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
