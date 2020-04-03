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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.FileFilter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;

import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.ICommitterRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.commons.lang.io.CachedInputStream;
import com.norconex.commons.lang.map.Properties;

/**
 * File sytem queue utility methods.
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public final class FSQueueUtil {

    static final String EXT = ".zip";
    static final FileFilter FILTER = f -> f.getName().endsWith(EXT);

    private FSQueueUtil() {
        super();
    }

    public static void toZipFile(
            ICommitterRequest request, Path targetFile) throws IOException {

        try (ZipOutputStream zipOS = new ZipOutputStream(
                IOUtils.buffer(Files.newOutputStream(targetFile)), UTF_8)) {
            // Reference
            zipOS.putNextEntry(new ZipEntry("reference"));
            IOUtils.write(request.getReference(), zipOS, UTF_8);
            zipOS.flush();
            zipOS.closeEntry();

            // Metadata
            zipOS.putNextEntry(new ZipEntry("metadata"));
            request.getMetadata().storeToProperties(zipOS);
            zipOS.flush();
            zipOS.closeEntry();

            // Content
            if (request instanceof UpsertRequest) {
                UpsertRequest upsert = (UpsertRequest) request;
                zipOS.putNextEntry(new ZipEntry("content"));
                IOUtils.copy(upsert.getContent(), zipOS);
                zipOS.flush();
                zipOS.closeEntry();
            }
        }
    }

    public static ICommitterRequest fromZipFile(Path sourceFile) {
        String ref = null;
        Properties meta = new Properties();
        CachedInputStream content = null;

        try (ZipInputStream zipIn = new ZipInputStream(
                IOUtils.buffer(Files.newInputStream(sourceFile)))) {
            ZipEntry zipEntry = zipIn.getNextEntry();
            while (zipEntry != null) {
                String name = zipEntry.getName();
                if ("reference".equals(name)) {
                    ref = IOUtils.toString(zipIn, StandardCharsets.UTF_8);
                } else if ("metadata".equals(name)) {
                    meta.loadFromProperties(zipIn);
                } else if ("content".equals(name)) {
                    content = CachedInputStream.cache(zipIn);
                    content.enforceFullCaching();
                }
                zipIn.closeEntry();
                zipEntry = zipIn.getNextEntry();
            }
            zipIn.closeEntry();
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Could not convert committer request file to object.", e);
        }
        if (content == null) {
            return new DeleteRequest(ref, meta);
        }
        return new UpsertRequest(ref, meta, content);
    }
}
