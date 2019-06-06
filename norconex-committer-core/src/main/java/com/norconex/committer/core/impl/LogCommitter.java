/* Copyright 2019 Norconex Inc.
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
package com.norconex.committer.core.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norconex.committer.core.ICommitter;
import com.norconex.commons.lang.SLF4JUtil;
import com.norconex.commons.lang.map.Properties;

/**
 * <p>
 * <b>WARNING: Not intended for production use.</b>
 * </p>
 * <p>
 * A Committer that logs all data associated with every document, added or
 * removed, to the application logs, or the console (STDOUT/STDERR). Default
 * uses application logger with INFO log level.
 * </p>
 * <p>
 * This Committer can be useful for troubleshooting.  Given how much
 * information this could represent, it is recommended
 * you do not use in a production environment. At a minimum, if you are
 * logging to file, make sure to rotate/clean the logs regularly.
 * </p>
 * <p>
 * XML configuration usage:
 * </p>
 * <pre>
 *  &lt;committer class="com.norconex.committer.core.impl.LogCommitter"&gt;
 *      &lt;logLevel&gt;[TRACE|DEBUG|INFO|WARN|ERROR|STDOUT|STDERR]&lt;logLevel/&gt;
 *      &lt;fieldsRegex&gt;
 *          (Regular expressions matching fields to log. Default logs all.)
 *      &lt;fieldsRegex/&gt;
 *      &lt;ignoreContent&gt;[false|true]&lt;ignoreContent/&gt;
 *  &lt;/committer&gt;
 * </pre>
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public class LogCommitter implements ICommitter  {

    private static final Logger LOG =
            LoggerFactory.getLogger(LogCommitter.class);

    private static final int LOG_TIME_BATCH_SIZE = 100;

    private long addCount = 0;
    private long removeCount = 0;
    private final StopWatch watch = new StopWatch();

    private boolean ignoreContent;
    private String fieldsRegex;
    private String logLevel;

    public boolean isIgnoreContent() {
        return ignoreContent;
    }
    public void setIgnoreContent(boolean ignoreContent) {
        this.ignoreContent = ignoreContent;
    }

    public String getFieldsRegex() {
        return fieldsRegex;
    }
    public void setFieldsRegex(String fieldsRegex) {
        this.fieldsRegex = fieldsRegex;
    }

    public String getLogLevel() {
        return logLevel;
    }
    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    /**
     * Constructor.
     */
    public LogCommitter() {
        super();
        watch.start();
    }

    @Override
    public synchronized void add(
            String reference, InputStream content, Properties metadata) {

        StringBuilder b = new StringBuilder();
        b.append("=== DOCUMENT ADDED ==================================\n");

        stringifyRefAndMeta(b, reference, metadata);

        if (!ignoreContent) {
            b.append("--- Content -----------------------------------------\n");
            try {
                b.append(IOUtils.toString(
                        content, StandardCharsets.UTF_8)).append('\n');
            } catch (IOException e) {
                b.append(ExceptionUtils.getStackTrace(e));
            }
        }
        log(b.toString());

        addCount++;
        if (addCount % LOG_TIME_BATCH_SIZE == 0) {
            LOG.info("{} additions queued in: {}", addCount, watch.toString());
        }
    }

    @Override
    public synchronized void remove(String reference, Properties metadata) {

        StringBuilder b = new StringBuilder();
        b.append("=== DOCUMENT REMOVED ================================\n");
        stringifyRefAndMeta(b, reference, metadata);
        log(b.toString());

        removeCount++;
        if (removeCount % LOG_TIME_BATCH_SIZE == 0) {
            LOG.info("{} deletions queued in {}", removeCount, watch);
        }
    }

    private void stringifyRefAndMeta(
            StringBuilder b, String reference, Properties metadata) {
        b.append("REFERENCE = ").append(reference).append('\n');
        if (metadata != null) {
            b.append("--- Metadata: ---------------------------------------\n");
            for (Entry<String, List<String>> en : metadata.entrySet()) {
                if (StringUtils.isBlank(fieldsRegex)
                        || en.getKey().matches(fieldsRegex)) {
                    for (String val : en.getValue()) {
                        b.append(en.getKey()).append(" = ")
                                .append(val).append('\n');
                    }
                }

            }
        }
    }

    private void log(String txt) {
        String lvl = Optional.ofNullable(logLevel).orElse("INFO").toUpperCase();
        if ("STDERR".equals(lvl)) {
            System.err.println(txt);
        } else if ("STDOUT".equals(lvl)) {
            System.out.println(txt);
        } else {
            SLF4JUtil.log(LOG, lvl, txt);
        }
    }

    @Override
    public void commit() {
        LOG.info("{} additions committed.", addCount);
        LOG.info("{} deletions committed.", removeCount);
        LOG.info("Total elapsed time: {}", watch);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof LogCommitter)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .toString();
    }
}
