/* Copyright 2010-2013 Norconex Inc.
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
package com.norconex.committer;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.norconex.committer.impl.FileSystemCommitter;
import com.norconex.commons.lang.io.FileUtil;
import com.norconex.commons.lang.io.IFileVisitor;
import com.norconex.commons.lang.map.Properties;

/**
 * Base batching implementation queuing documents on filesystem.
 * 
 * @author Pascal Essiembre
 * @deprecated Since 1.1.0. Use {@link AbstractFileQueueCommitter}
 */
@Deprecated
@SuppressWarnings("nls")
public abstract class FileSystemQueueCommitter extends BatchableCommitter {

    private static final long serialVersionUID = -5775959203678116077L;

    /** Default queue directory. */
    public static final String DEFAULT_QUEUE_DIR = "./committer-queue";

    private static final FileFilter NON_META_FILTER = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            return !pathname.getName().endsWith(".meta");
        }
    };
    

    private final FileSystemCommitter queue = new FileSystemCommitter();

    /**
     * Constructor.
     */
    public FileSystemQueueCommitter() {
        super();
        queue.setDirectory(DEFAULT_QUEUE_DIR);
    }
    /**
     * Constructor.
     * @param batchSize batch size
     */
    public FileSystemQueueCommitter(int batchSize) {
        super(batchSize);
        queue.setDirectory(DEFAULT_QUEUE_DIR);
    }

    /**
     * Gets the queue directory.
     * @return queue directory
     */
    public String getQueueDir() {
        return queue.getDirectory();
    }
    /**
     * Sets the queue directory.
     * @param queueDir queue directory
     */
    public void setQueueDir(String queueDir) {
        this.queue.setDirectory(queueDir);
    }

    @Override
    protected void queueBatchableAdd(
            String reference, File document, Properties metadata) {
        queue.queueAdd(reference, document, metadata);
    }

    @Override
    protected void queueBatchableRemove(
            String ref, File document, Properties metadata) {
        queue.queueRemove(ref, document, metadata);
    }

    @Override
    public void commit() {

        // --- Additions ---
        FileUtil.visitAllFiles(queue.getAddDir(), new IFileVisitor() {
            @Override
            public void visit(File file) {
                try {
                    QueuedAddedDocument doc = new QueuedAddedDocument(file);
                    preCommitAddedDocument(doc);
                    commitAddedDocument(doc);
                } catch (IOException e) {
                    throw new CommitterException(
                            "Cannot create document for file: " + file, e);
                }
            }
        }, NON_META_FILTER);

        // --- Deletions ---
        FileUtil.visitAllFiles(queue.getRemoveDir(), new IFileVisitor() {
            @Override
            public void visit(File file) {
                try {
                    QueuedDeletedDocument doc = new QueuedDeletedDocument(file);
                    preCommitDeletedDocument(doc);
                    commitDeletedDocument(doc);
                } catch (IOException e) {
                    throw new CommitterException(
                            "Cannot read reference from : " + file, e);
                }
            }
        });

        commitComplete();
    }

    /**
     * <p>
     * Allow subclasses to commit a file to be added.
     * </p>
     * <p>
     * The subclass has the responsibility of deleting the file once the content
     * is permanently stored by invoking 
     * {@link QueuedAddedDocument#deleteFromQueue()}. 
     * The subclass may decide to further batch those documents before
     * storing them if more efficient this way.
     * </p>
     *
     * @param document the document to commit
     * @throws IOException
     */
    protected abstract void commitAddedDocument(QueuedAddedDocument document)
            throws IOException;

    /**
     * <p>
     * Allow subclasses to commit a file to be deleted.
     * </p>
     * <p>
     * The subclass has the responsibility of deleting the file once the content
     * is permanently stored by invoking 
     * {@link QueuedDeletedDocument#deleteFromQueue()}. The subclass may 
     * decide to further batch those deletions before storing them if more
     * efficient that way.
     * </p>
     *
     * @param document the document to commit
     * @throws IOException
     */
    protected abstract void commitDeletedDocument(
            QueuedDeletedDocument document)
            throws IOException;

    /**
     * Allow subclasses to operate upon the end of the commit operation.
     * 
     * For example, if the subclass decided to batch documents to commit, it may
     * decide to store all remaining documents on that event.
     * 
     */
    protected abstract void commitComplete();

    /**
     * Optionally performs operations on a document to be added before
     * actually committing it.  Default implementation does nothing.
     * @param document document to be added
     * @throws IOException
     */
    protected void preCommitAddedDocument(QueuedAddedDocument document) 
            throws IOException {
        // Do nothing by default
    }
    /**
     * Optionally performs operations on a document to be deleted before
     * actually committing it.  Default implementation does nothing.
     * @param document document to be deleted
     * @throws IOException
     */
    protected void preCommitDeletedDocument(QueuedDeletedDocument document)
        throws IOException {
        // Do nothing by default
    }
    
    private Properties loadMetadata(File file) throws IOException {
        Properties metadata = new Properties();
        File metaFile = new File(file.getAbsolutePath() + ".meta");
        if (metaFile.exists()) {
            FileInputStream is = new FileInputStream(metaFile);
            metadata.load(is);
            IOUtils.closeQuietly(is);
        }
        return metadata;
    }

    /**
     * Class representing a document addition.
     */
    public final class QueuedAddedDocument implements Serializable {
        private static final long serialVersionUID = 3695149319924730851L;
        private final File file;
        private final Properties metadata;

        private QueuedAddedDocument(File file) throws IOException {
            this.file = file;
            metadata = loadMetadata(file);
        }
        /**
         * Gets the metadata.
         * @return metadata
         */
        public Properties getMetadata() {
            return metadata;
        }
        /**
         * Gets the content as a stream.
         * @return content stream
         * @throws IOException problem obtaining content stream
         */
        public InputStream getContentStream() throws IOException {
            return new FileInputStream(file);
        }
        /**
         * Deletes the document from queue.
         */
        public void deleteFromQueue() {
            //TODO use FileUtil.deleteFile(file) ??
            File metaFile = new File(file.getAbsolutePath() + ".meta");
            metaFile.delete();
            file.delete();
        }
    }

    /**
     * Class representing a document deletion.
     */
    public final class QueuedDeletedDocument implements Serializable {
        private static final long serialVersionUID = -4494846630316833501L;
        private final File file;
        private final String reference;

        private QueuedDeletedDocument(File file) throws IOException {
            super();
            this.file = file;
            reference = FileUtils.readFileToString(file);
        }
        /**
         * Document reference.
         * @return reference
         */
        public String getReference() {
            return reference;
        }
        /**
         * Deletes document from queue.
         */
        public void deleteFromQueue() {
            //TODO use FileUtil.deleteFile(file) ??
            file.delete();
        }
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder();
        hashCodeBuilder.append(queue);
        return hashCodeBuilder.toHashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof FileSystemQueueCommitter)) {
            return false;
        }
        FileSystemQueueCommitter other = (FileSystemQueueCommitter) obj;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(queue, other.queue);
        return equalsBuilder.isEquals();
    }
    
    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append("queue", queue);
        return builder.toString();
    }
}
