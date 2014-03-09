/* Copyright 2010-2014 Norconex Inc.
 * 
 * This file is part of Norconex Committer.
 * 
 * Norconex Committer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * Norconex Committer is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Norconex Committer. If not, see <http://www.gnu.org/licenses/>.
 */
package com.norconex.committer.impl;

import java.io.File;

import org.apache.commons.lang.time.StopWatch;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.ICommitter;
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
 *  &lt;committer class="com.norconex.committer.impl.NilCommitter" /&gt;
 * </pre>
 * @author Pascal Essiembre
 */
public class NilCommitter implements ICommitter  {

    private static final long serialVersionUID = -8276220684343961238L;
    private static final Logger LOG = LogManager.getLogger(NilCommitter.class);
    
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
    public synchronized void queueAdd(
            String reference, File document, Properties metadata) {
        addCount++;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Queing addition of " + reference);
        }
        if (addCount % 100 == 0) {
            LOG.info(addCount + " additions queued in: " + watch.toString());
        }
    }

    @Override
    public synchronized void queueRemove(
            String reference, File document, Properties metadata) {
        removeCount++;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Queing deletion of " + reference);
        }
        if (removeCount % 100 == 0) {
            LOG.info(removeCount + " deletions queued in " + watch.toString());
        }
    }

    @Override
    public void commit() {
        LOG.info(addCount + " additions committed.");
        LOG.info(removeCount + " deletions committed.");
        LOG.info("Total elapsed time: " + watch.toString());
    }

}
