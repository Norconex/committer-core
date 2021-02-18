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
package com.norconex.committer.core3.fs;

import static com.norconex.commons.lang.file.FileUtil.toSafeFileName;
import static org.apache.commons.lang3.StringUtils.stripToEmpty;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norconex.committer.core3.AbstractCommitter;
import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.commons.lang.xml.IXMLConfigurable;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>
 * Base class for committers writing to the local file system.
 * </p>
 *
 * <h3>XML configuration usage:</h3>
 * <p>
 * The following are configuration options inherited by subclasses:
 * </p>
 * {@nx.xml #options
 *   <directory>(path where to save the files)</directory>
 *   <docsPerFile>(max number of docs per file)</docsPerFile>
 *   <compress>[false|true]</compress>
 *   <splitUpsertDelete>[false|true]</splitUpsertDelete>
 *   <fileNamePrefix>(optional prefix to created file names)</fileNamePrefix>
 *   <fileNameSuffix>(optional suffix to created file names)</fileNameSuffix>
 *   {@nx.include com.norconex.committer.core3.AbstractCommitter@nx.xml.usage}
 * }
 *
 * @param <T> type of file serializer
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public abstract class AbstractFSCommitter<T> extends AbstractCommitter
        implements IXMLConfigurable  {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractFSCommitter.class);

    // These will share the same instance if not split.
    private DocWriterHandler upsertHandler;
    private DocWriterHandler deleteHandler;

    // Configurable
    private Path directory;
    private int docsPerFile;
    private boolean compress;
    private boolean splitUpsertDelete;
    private String fileNamePrefix;
    private String fileNameSuffix;

    /**
     * Gets the directory where files are committed.
     * @return directory
     */
    public Path getDirectory() {
        return directory;
    }
    public void setDirectory(Path directory) {
        this.directory = directory;
    }

    public int getDocsPerFile() {
        return docsPerFile;
    }
    public void setDocsPerFile(int docsPerFile) {
        this.docsPerFile = docsPerFile;
    }

    public boolean isCompress() {
        return compress;
    }
    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    public boolean isSplitUpsertDelete() {
        return splitUpsertDelete;
    }
    public void setSplitUpsertDelete(boolean separateAddDelete) {
        this.splitUpsertDelete = separateAddDelete;
    }

    /**
     * Gets the file name prefix (default is <code>null</code>).
     * @return file name prefix
     */
    public String getFileNamePrefix() {
        return fileNamePrefix;
    }
    /**
     * Sets an optional file name prefix.
     * @param fileNamePrefix file name prefix
     */
    public void setFileNamePrefix(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
    }

    /**
     * Gets the file name suffix (default is <code>null</code>).
     * @return file name suffix
     */
    public String getFileNameSuffix() {
        return fileNameSuffix;
    }
    /**
     * Sets an optional file name suffix.
     * @param fileNameSuffix file name suffix
     */
    public void setFileNameSuffix(String fileNameSuffix) {
        this.fileNameSuffix = fileNameSuffix;
    }

    @Override
    protected void doInit() throws CommitterException {
        if (directory == null) {
            this.directory = getCommitterContext().getWorkDir();
        }

        try {
            FileUtils.forceMkdir(directory.toFile());
        } catch (IOException e) {
            throw new CommitterException("Could not create directory: "
                    + directory.toAbsolutePath(), e);
        }

        String fileBaseName = DateFormatUtils.format(
                System.currentTimeMillis(), "yyyy-MM-dd'T'hh-mm-ss-SSS");
        if (splitUpsertDelete) {
            this.upsertHandler = new DocWriterHandler("upsert-" + fileBaseName);
            this.deleteHandler = new DocWriterHandler("delete-" + fileBaseName);
        } else {
            // when using same file for both upsert and delete, share instance.
            this.upsertHandler = new DocWriterHandler(fileBaseName);
            this.deleteHandler = upsertHandler;
        }
    }
    @Override
    protected synchronized void doUpsert(UpsertRequest upsertRequest)
            throws CommitterException {
        try {
            writeUpsert(upsertHandler.getDocWriter(), upsertRequest);
        } catch (IOException e) {
            throw new CommitterException("Could not write upsert request for: "
                    + upsertRequest.getReference());
        }
    }
    @Override
    protected synchronized void doDelete(DeleteRequest deleteRequest)
            throws CommitterException {
        try {
            writeDelete(deleteHandler.getDocWriter(), deleteRequest);
        } catch (IOException e) {
            throw new CommitterException("Could not write delete request for: "
                    + deleteRequest.getReference());
        }
    }
    @Override
    protected void doClose() throws CommitterException {
        try {
            if (upsertHandler != null) {
                upsertHandler.close();
            }
            if (deleteHandler != null
                    && !Objects.equals(deleteHandler, upsertHandler)) {
                deleteHandler.close();
            }
        } catch (IOException e) {
            throw new CommitterException("Could not close file writer.", e);
        }
    }

    @Override
    protected void doClean() throws CommitterException {
        // NOOP, no internal state is kept.
        // We do not clean previously committed files.
    }

    @Override
    public final void loadCommitterFromXML(XML xml) {
        loadFSCommitterFromXML(xml);
        setDirectory(xml.getPath("directory", directory));
        setDocsPerFile(xml.getInteger("docsPerFile", docsPerFile));
        setCompress(xml.getBoolean("compress", compress));
        setSplitUpsertDelete(
                xml.getBoolean("splitUpsertDelete", splitUpsertDelete));
        setFileNamePrefix(xml.getString("fileNamePrefix", fileNamePrefix));
        setFileNameSuffix(xml.getString("fileNameSuffix", fileNameSuffix));
    }
    @Override
    public final void saveCommitterToXML(XML xml) {
        saveFSCommitterToXML(xml);
        xml.addElement("directory", directory);
        xml.addElement("docsPerFile", docsPerFile);
        xml.addElement("compress", compress);
        xml.addElement("splitUpsertDelete", splitUpsertDelete);
        xml.addElement("fileNamePrefix", fileNamePrefix);
        xml.addElement("fileNameSuffix", fileNameSuffix);
    }

    public void loadFSCommitterFromXML(XML xml) {
        //NOOP
    }
    public void saveFSCommitterToXML(XML xml) {
        //NOOP
    }


    protected abstract String getFileExtension();
    protected abstract T createDocWriter(Writer writer) throws IOException;
    protected abstract void writeUpsert(
            T docWriter, UpsertRequest upsertRequest) throws IOException;
    protected abstract void writeDelete(
            T docWriter, DeleteRequest deleteRequest) throws IOException;
    protected abstract void closeDocWriter(T docWriter)
            throws IOException;

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

    private class DocWriterHandler implements AutoCloseable {
        private final String fileBaseName;
        private int writeCount;
        // start file numbering at 1
        private int fileNumber = 1;
        private File file;
        private T docWriter;
        private Writer writer = null;
        public DocWriterHandler(String fileBaseName) {
            super();
            this.fileBaseName = fileBaseName;
        }
        private synchronized T getDocWriter() throws IOException {

            boolean docPerFileReached =
                    docsPerFile > 0 && writeCount == docsPerFile;

            if (docPerFileReached) {
                close();
            }

            // invocation count is zero or max reached, we need a new file.
            if (writeCount == 0) {
                file = new File(directory.toFile(), buildFileName());
                LOG.info("Creating file: {}", file);
                if (compress) {
                    writer = new OutputStreamWriter(new GZIPOutputStream(
                            new FileOutputStream(file), true));
                } else {
                    writer = new FileWriter(file);
                }
                docWriter = createDocWriter(writer);
                fileNumber++;
            }
            writeCount++;
            return docWriter;
        }
        private String buildFileName() {
            String fileName = stripToEmpty(toSafeFileName(fileNamePrefix))
                  + fileBaseName + stripToEmpty(toSafeFileName(fileNameSuffix))
                  + "_" + fileNumber + "." + getFileExtension();
            if (compress) {
                fileName += ".gz";
            }
            return fileName;
        }
        @Override
        public synchronized void close() throws IOException {
            writeCount = 0;
            try {
                closeDocWriter(docWriter);
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }
            file = null;
            writer = null;
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
}
