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
import static org.apache.commons.lang3.StringUtils.leftPad;

import java.io.FileFilter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.commons.collections4.iterators.TransformIterator;
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
import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.ICommitterRequest;
import com.norconex.committer.core3.batch.IBatchConsumer;
import com.norconex.committer.core3.batch.queue.ICommitterQueue;
import com.norconex.commons.lang.TimeIdGenerator;
import com.norconex.commons.lang.collection.CountingIterator;
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
 * for failing batches.
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
 *      execution (e.g., prematurely ended).
 *   </commitLeftoversOnInit>
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

    static final String EXT = ".zip";
    static final FileFilter FILTER = f -> f.getName().endsWith(EXT);

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
            IBatchConsumer batchConsumer) throws CommitterException {

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
        } catch (IOException e) {
            throw new CommitterException(
                    "Could not create committer queue directory: "
                            + workDir.toAbsolutePath());
        }

        if (commitLeftoversOnInit) {
            // Resume by first processing existing batches not yet committed
            // from previous execution.
            // "false" by default since we do not want to commit leftovers
            // when doing initialization for a "clean" operation.
            LOG.info("Committing any leftovers...");
            int cnt = consumeBatch(queueDir);
            if (cnt == 0) {
                LOG.info("No leftovers.");
            } else {
                LOG.info("{} leftovers committed.", cnt);
            }
        }

        // Start for real
        this.activeDir = createActiveDir();

        LOG.info("File system Committer queue initialized.");
    }

    public IBatchConsumer getBatchConsumer() {
        return batchConsumer;
    }
    public int getBatchSize() {
        return batchSize;
    }
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
    public int getMaxPerFolder() {
        return maxPerFolder;
    }
    public void setMaxPerFolder(int maxPerFolder) {
        this.maxPerFolder = maxPerFolder;
    }
    public boolean isCommitLeftoversOnInit() {
        return commitLeftoversOnInit;
    }
    public void setCommitLeftoversOnInit(boolean commitLeftoversOnInit) {
        this.commitLeftoversOnInit = commitLeftoversOnInit;
    }

    @Override
    public void queue(ICommitterRequest request) throws CommitterException {

        MutableObject<Path> fullBatchDir = new MutableObject<>();

        Path file = createQueueFile(request, fullBatchDir);

        try {
            FSQueueUtil.toZipFile(request, file);
        } catch (IOException e) {
            throw new CommitterException("Could not queue request for "
                    + request.getReference() + " at "
                    + file.toAbsolutePath(), e);
        }

        if (fullBatchDir.getValue() != null) {
            consumeBatch(fullBatchDir.getValue());
        }
    }

    @Override
    public void loadFromXML(XML xml) {
        setBatchSize(xml.getInteger("batchSize", batchSize));
        setMaxPerFolder(xml.getInteger("maxPerFolder", maxPerFolder));
        setCommitLeftoversOnInit(
                xml.getBoolean("commitLeftoversOnInit", commitLeftoversOnInit));

    }
    @Override
    public void saveToXML(XML xml) {
        xml.addElement("batchSize", batchSize);
        xml.addElement("maxPerFolder", maxPerFolder);
        xml.addElement("commitLeftoversOnInit", commitLeftoversOnInit);
    }

    // delete directory/files after read
    //
    private int consumeBatch(Path dir) throws CommitterException {
        CountingIterator<ICommitterRequest> it;
        try (Stream<Path> s = Files.find(dir,  Integer.MAX_VALUE,
                (f, a) -> f.toFile().getName().endsWith(EXT))) {
            it = new CountingIterator<>(new TransformIterator<>(
                    s.iterator(), this::loadCommitterRequest));
            if (it.hasNext()) {
                batchConsumer.consume(it);
            }
        } catch (IOException | UncheckedIOException e) {
            //TODO do moving the files to error folder or let committer do it?
            try {
                FileUtils.moveDirectoryToDirectory(
                        dir.toFile(), errorDir.toFile(), true);
            } catch (IOException e1) {
                throw new CommitterException("Could not process committer batch "
                        + "located at " + dir.toAbsolutePath()
                        + " and could not copy it under "
                        + errorDir.toAbsolutePath(), e);
            }
            throw new CommitterException("Could not process committer batch "
                    + "located at " + dir.toAbsolutePath(), e);
        }
        try {
            FileUtil.delete(dir.toFile());
        } catch (IOException e) {
            throw new CommitterException("Could not delete consumed committer "
                    + "batch located at " + dir.toAbsolutePath(), e);
        }
        return it.getCount();
    }

    private ICommitterRequest loadCommitterRequest(Path file) {
        try {
            return FSQueueUtil.fromZipFile(file, streamFactory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    @Override
    public void close() throws CommitterException {
        // specifying parent dir will process all that's left there.
        if (queueDir != null && Files.exists(queueDir)) {
            consumeBatch(queueDir);
        }
    }

    @Override
    public void clean() throws CommitterException {
        if (queueDir == null) {
            LOG.error("Queue directory not found. Nothing not clean.");
            return;
        }

        // move one level higher before deleting to get queue/active/errors
        try {
            FileUtil.delete(queueDir.getParent().toFile());
        } catch (IOException e) {
            throw new CommitterException("Could not clean queue "
                    + "directory located at "
                    + queueDir.getParent().toAbsolutePath(), e);
        }
    }

    private Path createActiveDir() {
        return queueDir.resolve("batch-" + TimeIdGenerator.next());
    }

    private synchronized Path createQueueFile(
            ICommitterRequest req, MutableObject<Path> consumeBatchDir)
                    throws CommitterException {
        try {
            // If starting a new batch, create the folder for it
            if (batchCount.get() == batchSize) {
                consumeBatchDir.setValue(activeDir);
                batchCount.set(0);
                activeDir = createActiveDir();
                Files.createDirectories(activeDir);
            }
        } catch (IOException e) {
            throw new CommitterException("Could not create batch directory: "
                    + activeDir.toAbsolutePath());
        }

        Path file = activeDir.resolve(filePath(batchCount.get())
                + (req instanceof DeleteRequest ? "-delete" : "-upsert") + EXT);
        try {
            Files.createDirectories(file.getParent());
        } catch (IOException e) {
            throw new CommitterException("Could not create file directory: "
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
