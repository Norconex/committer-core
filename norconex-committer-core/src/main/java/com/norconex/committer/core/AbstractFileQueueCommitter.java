/* Copyright 2010-2018 Norconex Inc.
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
package com.norconex.committer.core;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norconex.committer.core.impl.FileSystemCommitter;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.file.FileUtil;
import com.norconex.commons.lang.file.IFileVisitor;
import com.norconex.commons.lang.map.Properties;

//TODO Maybe offer pluggable implementations for where to queue (FS, DB, etc)?


/**
 * Queues documents on filesystem, leaving only the committing of additions
 * and deletions to implement.  Subclasses can optionally implement
 * {@link #prepareCommitAddition(IAddOperation)} and
 * {@link #prepareCommitDeletion(IDeleteOperation)} to manipulate the
 * data supplied with the operations before committing takes place.
 * <br><br>
 * To also control how many documents are sent on each call to
 * a remote repository, consider extending {@link AbstractBatchCommitter}.
 *
 * <p>Subclasses implementing {@link IXMLConfigurable} should allow this inner
 * configuration:</p>
 * <pre>
 *      &lt;queueDir&gt;(optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueSize&gt;(max queue size before committing)&lt;/queueSize&gt;
 * </pre>
 *
 * @author Pascal Essiembre
 * @since 1.1.0
 */
@SuppressWarnings("nls")
public abstract class AbstractFileQueueCommitter extends AbstractCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(
            AbstractFileQueueCommitter.class);

    /** Default directory where to queue files. */
    public static final String DEFAULT_QUEUE_DIR = "committer-queue";

    private static final int EMPTY_DIRS_SECONDS_LIMIT = 10;

    private static final FileFilter REF_FILTER = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            return pathname.getName().endsWith(
                    FileSystemCommitter.EXTENSION_REFERENCE);
        }
    };

    private final FileSystemCommitter queue = new FileSystemCommitter();

    /**
     * Files currently being committed
     */
    protected final ConcurrentHashMap<File, Thread> filesCommitting =
            new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    public AbstractFileQueueCommitter() {
        super();
        queue.setDirectory(DEFAULT_QUEUE_DIR);
    }
    /**
     * Constructor.
     * @param queueSize max queue size
     */
    public AbstractFileQueueCommitter(int queueSize) {
        super(queueSize);
        queue.setDirectory(DEFAULT_QUEUE_DIR);
    }

    /**
     * Gets the directory where queued files are stored.
     * @return directory
     */
    public String getQueueDir() {
        return queue.getDirectory();
    }
    /**
     * Sets the directory where queued files are stored.
     * @param queueDir directory
     */
    public void setQueueDir(String queueDir) {
        this.queue.setDirectory(queueDir);
    }

    @Override
    protected long getInitialQueueDocCount() {
        final MutableLong fileCount = new MutableLong();

        // --- Additions and Deletions ---
        FileUtil.visitAllFiles(
                new File(queue.getDirectory()), new IFileVisitor() {
            @Override
            public void visit(File file) {
                fileCount.increment();
            }
        }, REF_FILTER);
        return fileCount.longValue();
    }

    @Override
    protected void queueAddition(String reference, InputStream content,
            Properties metadata) {
        queue.add(reference, content, metadata);
    }

    @Override
    protected void queueRemoval(String ref, Properties metadata) {
        queue.remove(ref, metadata);
    }

    @Override
    public void commit() {

        // Get all files to be committed, relying on natural ordering which
        // will be in file creation order.
        final Queue<File> filesPending = new ConcurrentLinkedQueue<>();
        FileUtil.visitAllFiles(
                new File(queue.getDirectory()), new IFileVisitor() {
            @Override
            public void visit(File file) {
                 filesPending.add(file);
            }
        }, REF_FILTER);


        // Nothing left to commit. This happens if multiple threads are
        // committing at the same time and no more files are available for the
        // current thread to commit. This should happen rarely in practice.
        if (filesPending.isEmpty()) {
            return;
        }

        // Don't commit more than queue size
        List<ICommitOperation> filesToCommit = new ArrayList<>();
        while (filesToCommit.size() < queueSize) {

            File file = filesPending.poll();

            // If no more files available in both list, quit loop. This happens
            // if multiple threads tries to commit at once and there is less
            // than queueSize files to commit. This should happen rarely in
            // practice.
            if (file == null) {
                break;
            }

            // Current thread tries to own this file. If the file is already own
            // by another thread, continue and attempt to grab another file.
            if (filesCommitting.putIfAbsent(
                    file, Thread.currentThread()) != null) {
                continue;
            }

            // A file might have already been committed and cleanup from
            // the map, but still returned by the directory listing. Ignore
            // those. It is important to make this check AFTER the current
            // thread got ownership of the file.
            if (!file.exists()) {
                continue;
            }

            // Current thread will be committing this file
            if (file.getAbsolutePath().contains(
                    FileSystemCommitter.FILE_SUFFIX_ADD)) {
                filesToCommit.add(new FileAddOperation(file));
            } else if (file.getAbsolutePath().contains(
                    FileSystemCommitter.FILE_SUFFIX_REMOVE)) {
                filesToCommit.add(new FileDeleteOperation(file));
            } else {
                LOG.error("Unsupported file to commit: " + file);
            }
        }

        if (LOG.isInfoEnabled()) {
            LOG.info(String.format("Committing %s files",
                    filesToCommit.size()));
        }
        for (ICommitOperation op : filesToCommit) {
            try {
                if (op instanceof FileAddOperation) {
                    prepareCommitAddition((IAddOperation) op);
                    commitAddition((IAddOperation) op);
                } else {
                    prepareCommitDeletion((IDeleteOperation) op);
                    commitDeletion((IDeleteOperation) op);
                }
            } catch (IOException e) {
                throw new CommitterException(
                        "Cannot read reference from : " + op, e);
            }
        }

        commitComplete();

        deleteEmptyOldDirs(new File(queue.getDirectory()));

        // Cleanup committed files from map that might have been deleted
        Enumeration<File> en = filesCommitting.keys();
        while (en.hasMoreElements()) {
            File file = en.nextElement();
            if (!file.exists()) {
                filesCommitting.remove(file);
            }
        }
    }

    /**
     * <p>
     * Allow subclasses to commit a file to be added.
     * </p>
     * <p>
     * The subclass has the responsibility of deleting the file once the content
     * is permanently stored.
     * The subclass may decide to further batch those documents before
     * storing them if more efficient this way.
     * </p>
     *
     * @param operation the document operation to perform
     * @throws IOException problem to commit addition
     */
    protected abstract void commitAddition(IAddOperation operation)
            throws IOException;

    /**
     * <p>
     * Allow subclasses to commit a file to be deleted.
     * </p>
     * <p>
     * The subclass has the responsibility of deleting the file once the content
     * is permanently stored. The subclass may
     * decide to further batch those deletions before storing them if more
     * efficient that way.
     * </p>
     *
     * @param operation the document operation to perform
     * @throws IOException problem committing deletion
     */
    protected abstract void commitDeletion(IDeleteOperation operation)
            throws IOException;

    /**
     * Allow subclasses to operate upon the end of the commit operation.
     *
     * For example, if the subclass decided to batch documents to commit, it may
     * decide to store all remaining documents on that event.
     */
    protected abstract void commitComplete();

    /**
     * Optionally performs actions on a document to be added before
     * actually committing it.  Default implementation does nothing.
     * @param operation addition to be performed
     * @throws IOException problem preparing commit addition
     */
    protected void prepareCommitAddition(IAddOperation operation)
            throws IOException {
        // Do nothing by default
    }
    /**
     * Optionally performs operations on a document to be deleted before
     * actually committing it.  Default implementation does nothing.
     * @param operation deletion to be performed
     * @throws IOException problem preparing commit deletion
     */
    protected void prepareCommitDeletion(IDeleteOperation operation)
        throws IOException {
        // Do nothing by default
    }

    // Remove empty dirs to avoid the above looping taking too long
    // when we are dealing with thousands/millions of documents
    // do it on files 10 seconds old to avoid threading conflicts
    private void deleteEmptyOldDirs(File parentDir) {
        final long someTimeAgo = System.currentTimeMillis()
                - (DateUtils.MILLIS_PER_SECOND * EMPTY_DIRS_SECONDS_LIMIT);
        Date date = new Date(someTimeAgo);
        int dirCount = FileUtil.deleteEmptyDirs(parentDir, date);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleted " + dirCount + " empty directories under "
                    + parentDir);
        }
    }

    @Override
    public boolean equals(final Object other) {
        return EqualsBuilder.reflectionEquals(this, other, "filesCommitting");
    }
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, "filesCommitting");
    }
    @Override
    public String toString() {
        return new ReflectionToStringBuilder(
                this, ToStringStyle.SHORT_PREFIX_STYLE).setExcludeFieldNames(
                        "filesCommitting").toString();
    }
}
