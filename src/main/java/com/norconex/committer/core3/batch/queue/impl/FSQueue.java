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
package com.norconex.committer.core3.batch.queue.impl;

import static java.lang.Math.ceil;
import static java.lang.Math.log;
import static java.lang.Math.log10;
import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.leftPad;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.EqualsExclude;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.HashCodeExclude;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringExclude;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.mutable.MutableObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norconex.committer.core3.CommitterContext;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.ICommitterRequest;
import com.norconex.committer.core3.batch.IBatchConsumer;
import com.norconex.committer.core3.batch.queue.CommitterQueueException;
import com.norconex.committer.core3.batch.queue.ICommitterQueue;
import com.norconex.commons.lang.TimeIdGenerator;
import com.norconex.commons.lang.collection.CountingIterator;
import com.norconex.commons.lang.exec.RetriableException;
import com.norconex.commons.lang.exec.Retrier;
import com.norconex.commons.lang.file.FileUtil;
import com.norconex.commons.lang.io.CachedStreamFactory;
import com.norconex.commons.lang.xml.IXMLConfigurable;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>
 * File System queue. Queues committer requests as zip files and deletes
 * successfully committed zip files.
 * </p>
 * <p>
 * The top-level queue directory is the one defined in the
 * {@link CommitterContext} initialization argument or the system
 * temporary directory if <code>null</code>. A "queue" sub-folder will be
 * created for queued requests, while an "error" one will also be created
 * for failed batches.
 * </p>
 *
 * <h3>Handling Failures</h3>
 * <p>
 * Before moving a failed batch to the "error" folder you have the opportunity
 * to have this queue tell the Committer to retry the batch.  This can be
 * particularly useful if you experience issues such as:
 * </p>
 * <ul>
 *   <li>The target repository momentarily failed to respond.</li>
 *   <li>A specific document was rejected by the target repository.</li>
 *   <li>The batch was too big for the repository to handle.</li>
 *   <li>Etc.</li>
 * </ul>
 * <p>
 * In addition to specifying how many time to retry and how long to wait
 * between each attempts, you can also break the batch size into smaller
 * chunks.  The "splitBatch" setting offers the following options:
 * </p>
 * <ul>
 *   <li><b>OFF</b> - Default. The failing batch is not split.</li>
 *   <li><b>HALF</b> - The batch is split in half at each failure
 *       (after maxRetries is reached), up until batches are of size 1
 *       (committing documents individually).</li>
 *   <li><b>ONE</b> - Documents from the failing batch are sent one by one.</li>
 * </ul>
 * <p>
 * When "splitBatch" is not OFF and used in combination with a non-zero
 * "maxRetry", the later then represents how many times to retry each new
 * smaller batches created.
 * </p>
 *
 * {@nx.xml.usage
 * <queue class="com.norconex.committer.core3.batch.queue.impl.FSQueue">
 *   <batchSize>
 *     (Optional number of documents queued after which we process a batch.
 *      Default is 20.)
 *   </batchSize>
 *   <maxPerFolder>
 *     (Optional maximum number of files or directories that can be queued
 *      in a single folder before a new one gets created. Default is 500.)
 *   </maxPerFolder>
 *   <commitLeftoversOnInit>
 *     (Optionally force to commit any leftover documents from a previous
 *      execution. E.g., prematurely ended.  Default is "false").
 *   </commitLeftoversOnInit>
 *   <onCommitFailure>
 *     <splitBatch>[OFF|HALF|ONE]</splitBatch>
 *     <maxRetries>
 *       (Max retries upon commit failures. Default is 0.)
 *     </maxRetries>
 *     <retryDelay>
 *       (Delay in milliseconds between retries. Default is 0.)
 *     </retryDelay>
 *     <ignoreErrors>
 *       [false|true]
 *       (When true, non-critical exceptions when interacting with the target
 *        repository won't be thrown to try continue the execution with other
 *        files to be committed. Instead, errors will be logged.
 *        In both cases the failing batch/files are moved to an
 *        "error" folder. Other types of exceptions may still be thrown.)
 *     </ignoreErrors>
 *   </onCommitFailure>
 * </queue>
 * }
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public class FSQueue implements ICommitterQueue, IXMLConfigurable {

    private static final Logger LOG = LoggerFactory.getLogger(FSQueue.class);

    public static final int DEFAULT_BATCH_SIZE = 20;
    public static final int DEFAULT_MAX_PER_FOLDER = 500;

    public enum SplitBatch { OFF, HALF, ONE }

    // defaults to system temp dir
    @ToStringExclude
    @EqualsExclude
    @HashCodeExclude
    private Path queueDir;
    @ToStringExclude
    @EqualsExclude
    @HashCodeExclude
    private Path errorDir;
    @ToStringExclude
    @EqualsExclude
    @HashCodeExclude
    private AtomicLong batchCount = new AtomicLong();

    // directory currently being written into (up to batch size).
    @ToStringExclude
    @EqualsExclude
    @HashCodeExclude
    private Path activeDir;

    // configurables;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private int maxPerFolder = DEFAULT_MAX_PER_FOLDER;
    private boolean commitLeftoversOnInit = false;
    private final Retrier retrier = new Retrier(0);
    private SplitBatch splitBatch = SplitBatch.OFF;
    private boolean ignoreErrors;

    @ToStringExclude
    @EqualsExclude
    @HashCodeExclude
    private IBatchConsumer batchConsumer;

    @ToStringExclude
    @EqualsExclude
    @HashCodeExclude
    private CachedStreamFactory streamFactory;

    @Override
    public void init(CommitterContext committerContext,
            IBatchConsumer batchConsumer) throws CommitterQueueException {

        this.batchConsumer = Objects.requireNonNull(batchConsumer,
                "'batchConsumer' must not be null.");

        LOG.info("Initializing file system Committer queue...");

        // Workdir:
        Path workDir = Optional.ofNullable(committerContext.getWorkDir())
                .orElseGet(() -> Paths.get(FileUtils.getTempDirectoryPath()));
        this.streamFactory = committerContext.getStreamFactory();

        LOG.info("Committer working directory: {}",
                workDir.toAbsolutePath());
        this.queueDir = workDir.resolve("queue");
        this.errorDir = workDir.resolve("error");
        try {
            FileUtils.forceMkdir(workDir.toFile());
            FileUtils.forceMkdir(queueDir.toFile());
            FileUtils.forceMkdir(errorDir.toFile());
            //TODO create README.txt in delete directory to explain
            // how files can be copied back if wanting to resume
            // them?

        } catch (IOException e) {
            throw new CommitterQueueException(
                    "Could not create committer queue directory: "
                            + workDir.toAbsolutePath());
        }

        if (commitLeftoversOnInit) {
            // Resume by first processing existing batches not yet committed
            // from previous execution.
            // "false" by default since we do not want to commit leftovers
            // when doing initialization for a "clean" operation.
            LOG.info("Committing any leftovers...");
            int cnt = consumeRemainingBatches();
            if (cnt == 0) {
                LOG.info("No leftovers.");
            } else {
                LOG.info("{} leftovers committed.", cnt);
            }
        }
        // else {
        //TODO Probably needs to read the directories to reset batch counter
        // to honor maxPerDoc
        //}

        // Start for real
        this.activeDir = createActiveDir();

        LOG.info("File system Committer queue initialized.");
    }

    public IBatchConsumer getBatchConsumer() {
        return batchConsumer;
    }
    /**
     * Gets the number of documents to be queued in a batch on disk before
     * consuming that batch.
     * @return batch size
     */
    public int getBatchSize() {
        return batchSize;
    }
    /**
     * Sets the number of documents to be queued in a batch on disk before
     * consuming that batch.
     * @param batchSize the batch size
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
    /**
     * Gets the maximum number of files to be queued on disk in a given folders.
     * A batch size can sometimes be too big for some file systems to handle
     * efficiently.  Having this number lower than the batch size allows
     * to have large batches without having too many files in a single
     * directory.
     * @return maximum number of files queued per directory
     */
    public int getMaxPerFolder() {
        return maxPerFolder;
    }
    /**
     * Sets the maximum number of files to be queued on disk in a given folders.
     * A batch size can sometimes be too big for some file systems to handle
     * efficiently.  Having this number lower than the batch size allows
     * to have large batches without having too many files in a single
     * directory.
     * @param maxPerFolder number of files queued per directory
     */
    public void setMaxPerFolder(int maxPerFolder) {
        this.maxPerFolder = maxPerFolder;
    }
    public boolean isCommitLeftoversOnInit() {
        return commitLeftoversOnInit;
    }
    public void setCommitLeftoversOnInit(boolean commitLeftoversOnInit) {
        this.commitLeftoversOnInit = commitLeftoversOnInit;
    }
    public int getMaxRetries() {
        return retrier.getMaxRetries();
    }
    public void setMaxRetries(int maxRetries) {
        this.retrier.setMaxRetries(maxRetries);
    }
    public long getRetryDelay() {
        return retrier.getRetryDelay();
    }
    public void setRetryDelay(long retryDelay) {
        this.retrier.setRetryDelay(retryDelay);
    }
    public SplitBatch getSplitBatch() {
        return splitBatch;
    }
    public void setSplitBatch(SplitBatch splitBatch) {
        this.splitBatch = splitBatch;
    }
    public boolean isIgnoreErrors() {
        return ignoreErrors;
    }
    public void setIgnoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
    }

    @Override
    public void queue(ICommitterRequest request)
            throws CommitterQueueException {

        MutableObject<Path> fullBatchDir = new MutableObject<>();

        Path file = createQueueFile(request, fullBatchDir);

        try {
            FSQueueUtil.toZipFile(request, file);
        } catch (IOException e) {
            throw new CommitterQueueException("Could not queue request for "
                    + request.getReference() + " at "
                    + file.toAbsolutePath(), e);
        }

        if (fullBatchDir.getValue() != null) {
            consumeBatchDirectory(fullBatchDir.getValue());
        }
    }

    // batch split(0) -> try(1) -> try(2) -> split(1) -> try(1) -> try(2) -> ...
    private int consumeBatchDirectory(Path dir) throws CommitterQueueException {
        try {
            return consumeSplitableBatchDirectory(dir);
        } catch (IOException e) {
            throw new CommitterQueueException("Could not read files to commit "
                    + "from directory " + dir.toAbsolutePath());
        }
    }
    private int consumeSplitableBatchDirectory(Path dir)
            throws CommitterQueueException, IOException {

        int totalConsumed = 0;
        int attemptDocConsumed = Math.max(1, batchSize);
        FSBatch batch = new FSBatch(streamFactory, dir, -1);
        boolean batchHadFailures = false;
        boolean batchRanSuccessfully = false;
        while (!FSQueueUtil.isEmpty(dir)) {
            try {
                int numConsumed = 0;
                batchRanSuccessfully = false;
                while ((numConsumed = consumeRetriableBatch(batch)) > 0) {
                    batchRanSuccessfully = true;
                    totalConsumed += numConsumed;
                }
            } catch (CommitterQueueException e) {
                batchHadFailures = true;
                attemptDocConsumed = reduceBatchSize(attemptDocConsumed);
                if (attemptDocConsumed == -1) {
                    moveUnrecoverableBatchError(batch, e);
                    break;
                } else {
                    batch = new FSBatch(streamFactory, dir, attemptDocConsumed);
                }
            }
        }

        if (batchHadFailures && batchRanSuccessfully) {
            LOG.info("Batch successfully recovered: {}", dir.toAbsolutePath());
        }

        // If an exception is thrown before reaching the following delete call,
        // it's OK since we do not want to delete a batch folder if there
        // were non-ignored exceptions in it.
        // In theory all files should be deleted or moved, but we call delete
        // here in case the folder itself remains.
        try {
            FileUtil.delete(dir.toFile());
        } catch (IOException e) {
            throw new CommitterQueueException(
                    "Could not delete consumed committer "
                            + "batch located at " + dir.toAbsolutePath(), e);
        }

        return totalConsumed;
    }

    private int consumeRetriableBatch(FSBatch batch)
            throws CommitterQueueException {
        try {
            return retrier.execute(() -> {
                CountingIterator<ICommitterRequest> it = batch.iterator();
                if (it.hasNext()) {
                    batchConsumer.consume(it);
                    // batch was consumed OK, delete it.
                    batch.delete();
                }
                return it.getCount();
            });
        } catch (RetriableException e) {
            throw new CommitterQueueException(
                    "Could not consume batch. Number of attempts: "
                            + (retrier.getMaxRetries() + 1), e);
        }
    }

    private void moveUnrecoverableBatchError(
            FSBatch batch, Exception e) throws CommitterQueueException {
        try {
            batch.move(errorDir);
        } catch (IOException e1) {
            throw new CommitterQueueException(
                      "Could not process one or more files form committer "
                    + "batch located at " + batch.getDir().toAbsolutePath()
                    + " and could not copy it under "
                    + errorDir.toAbsolutePath(), e);
        }
        String msg = "Could not process one or more files form committer batch "
                + "located at " + batch.getDir().toAbsolutePath()
                + ". Moved them to error directory: "
                + errorDir.toAbsolutePath();
        if (ignoreErrors) {
            LOG.error(msg, e);
        } else {
            throw new CommitterQueueException(msg, e);
        }
    }

    private int reduceBatchSize(int lastTriedSize) {
        // we do not got smaller than one, so we call it quit (-1)
        if (lastTriedSize <= 1) {
            return -1;
        }

        SplitBatch sb = ofNullable(splitBatch).orElse(SplitBatch.OFF);
        switch (sb) {
        case HALF:
            int newMaxSize = (lastTriedSize + 1) / 2;
            LOG.error("Could not process batch of max size {}. Trying "
                    + "again with max size {}...", lastTriedSize, newMaxSize);
            return newMaxSize;
        case ONE:
            LOG.error("Could not process batch of max size {}. Trying "
                    + "again one by one...", lastTriedSize);
            return 1;
        default:
            return -1;
        }
    }

    @Override
    public void loadFromXML(XML xml) {
        setBatchSize(xml.getInteger("batchSize", batchSize));
        setMaxPerFolder(xml.getInteger("maxPerFolder", maxPerFolder));
        setCommitLeftoversOnInit(
                xml.getBoolean("commitLeftoversOnInit", commitLeftoversOnInit));

        xml.ifXML("onCommitFailure", x -> {
            setMaxRetries(x.getInteger("maxRetries", retrier.getMaxRetries()));
            setRetryDelay(x.getLong("retryDelay", retrier.getRetryDelay()));
            setSplitBatch(x.getEnum(
                    "splitBatch", SplitBatch.class, splitBatch));
            setIgnoreErrors(x.getBoolean("ignoreErrors", isIgnoreErrors()));
        });
    }
    @Override
    public void saveToXML(XML xml) {
        xml.addElement("batchSize", batchSize);
        xml.addElement("maxPerFolder", maxPerFolder);
        xml.addElement("commitLeftoversOnInit", commitLeftoversOnInit);

        XML onFailXML = xml.addElement("onCommitFailure");
        onFailXML.addElement("maxRetries", retrier.getMaxRetries());
        onFailXML.addElement("retryDelay", retrier.getRetryDelay());
        onFailXML.addElement("splitBatch", splitBatch);
        onFailXML.addElement("ignoreErrors", ignoreErrors);
    }

    @Override
    public void close() throws CommitterQueueException {
        // specifying parent dir will process all that's left there.
        if (queueDir != null && Files.exists(queueDir)) {
            consumeRemainingBatches();
        }
    }

    private int consumeRemainingBatches() throws CommitterQueueException {
        // Process all batch dirs one by one:
        int cnt = 0;
        try (DirectoryStream<Path> directory =
                Files.newDirectoryStream(queueDir, Files::isDirectory)) {
            for (Path d : directory) {
                cnt += consumeBatchDirectory(d);
            }
        } catch (IOException e) {
            throw new CommitterQueueException("Could not consume "
                    + "remaining batches at "
                    + queueDir.toAbsolutePath(), e);
        }
        return cnt;
    }

    @Override
    public void clean() throws CommitterQueueException {
        if (queueDir == null) {
            LOG.error("Queue directory not found. Nothing not clean.");
            return;
        }

        // move one level higher before deleting to get queue/active/errors
        try {
            FileUtil.delete(queueDir.getParent().toFile());
        } catch (IOException e) {
            throw new CommitterQueueException("Could not clean queue "
                    + "directory located at "
                    + queueDir.getParent().toAbsolutePath(), e);
        }
    }

    private Path createActiveDir() {
        return queueDir.resolve("batch-" + TimeIdGenerator.next());
    }

    private synchronized Path createQueueFile(
            ICommitterRequest req, MutableObject<Path> consumeBatchDir)
                    throws CommitterQueueException {
        try {
            // If starting a new batch, create the folder for it
            if (batchCount.get() == batchSize) {
                consumeBatchDir.setValue(activeDir);
                batchCount.set(0);
                activeDir = createActiveDir();
                Files.createDirectories(activeDir);
            }
        } catch (IOException e) {
            throw new CommitterQueueException(
                    "Could not create batch directory: "
                            + activeDir.toAbsolutePath());
        }

        Path file = activeDir.resolve(filePath(batchCount.get())
                + (req instanceof DeleteRequest ? "-delete" : "-upsert")
                        + FSQueueUtil.EXT);
        try {
            Files.createDirectories(file.getParent());
        } catch (IOException e) {
            throw new CommitterQueueException(
                    "Could not create file directory: "
                            + file.toAbsolutePath());
        }
        batchCount.incrementAndGet();
        return file;
    }

    // use max batch size to figure out how many level of directories.
    // so we do not have more than "maxFilesPerFolder" docs in a given folder
    private String filePath(long value) {
        int nameLength = (int) ceil(log10(maxPerFolder));
        int dirDepth = (int) ceil(log(batchSize) / log(maxPerFolder));
        int pathLength = nameLength * dirDepth;
        String path = leftPad(Long.toString(value), pathLength, '0');
        return path.replaceAll(
                "(.{" + nameLength + "})(?!$)", "$1/");
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
