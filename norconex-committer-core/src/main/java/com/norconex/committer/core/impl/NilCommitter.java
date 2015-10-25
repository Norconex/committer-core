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
package com.norconex.committer.core.impl;

import java.io.InputStream;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.core.ICommitter;
import com.norconex.commons.lang.map.Properties;

/**
 * A dummy Committer that does nothing but logging to INFO (log4j) how many 
 * documents were queued (in batch of 100) so far, or committed (but fake).
 * Using log level DEBUG will print out all references being queued.
 * Can be useful for troubleshooting, or as a temporary replacement when 
 * developing or testing a product that uses Committers, but you want something
 * faster or you are not yet ready to turn yours "on".
 * <p>
 * XML configuration usage:
 * </p>
 * <pre>
 *  &lt;committer class="com.norconex.committer.core.impl.NilCommitter" /&gt;
 * </pre>
 * @author Pascal Essiembre
 */
public class NilCommitter implements ICommitter  {

    private static final Logger LOG = LogManager.getLogger(NilCommitter.class);
    
    private static final int LOG_TIME_BATCH_SIZE = 100;
    
    private long addCount = 0;
    private long removeCount = 0;
    private StopWatch watch = new StopWatch();
    
    /**
     * Constructor.
     */
    public NilCommitter() {
        super();
        watch.start();
    }

    @Override
    public synchronized void add(
            String reference, InputStream content, Properties metadata) {
        addCount++;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Queing addition of " + reference);
        }
        if (addCount % LOG_TIME_BATCH_SIZE == 0) {
            LOG.info(addCount + " additions queued in: " + watch.toString());
        }
    }

    @Override
    public synchronized void remove(String reference, Properties metadata) {
        removeCount++;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Queing deletion of " + reference);
        }
        if (removeCount % LOG_TIME_BATCH_SIZE == 0) {
            LOG.info(removeCount + " deletions queued in " + watch.toString());
        }
    }

    @Override
    public void commit() {
        LOG.info(addCount + " additions committed.");
        LOG.info(removeCount + " deletions committed.");
        LOG.info("Total elapsed time: " + watch.toString());
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof MultiCommitter)) {
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
