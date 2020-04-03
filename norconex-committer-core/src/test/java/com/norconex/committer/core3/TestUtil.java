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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;

import com.norconex.commons.lang.map.Properties;


/**
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public final class TestUtil {

    private TestUtil() {
        super();
    }

    public static Collection<File> listFSFiles(Path path) {
        return FileUtils.listFiles(path.toFile(),
                TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
    }
    public static Collection<File> listFSUpsertFiles(Path path) {
        return FileUtils.listFiles(path.toFile(),
                FileFilterUtils.prefixFileFilter("upsert-"),
                TrueFileFilter.INSTANCE);
    }
    public static Collection<File> listFSDeleteFiles(Path path) {
        return FileUtils.listFiles(path.toFile(),
                FileFilterUtils.prefixFileFilter("delete-"),
                TrueFileFilter.INSTANCE);
    }

    public static void commitRequests(ICommitter c, List<ICommitterRequest> crs)
            throws CommitterException {
        for (ICommitterRequest cr : crs) {
            commitRequest(c, cr);
        }
    }
    public static void commitRequest(ICommitter c, ICommitterRequest cr)
            throws CommitterException {
        if (cr instanceof UpsertRequest) {
            c.upsert((UpsertRequest) cr);
        } else {
            c.delete((DeleteRequest) cr);
        }
    }


    public static CommitterContext committerContext(Path folder) {
        return CommitterContext.build().setWorkDir(folder).create();
    }

    // 1+ = upserts; 0- = delete
    public static List<ICommitterRequest> mixedRequests(int... reqTypes) {
        List<ICommitterRequest> reqs = new ArrayList<>();
        for (int i = 0; i < reqTypes.length; i++) {
            if (reqTypes[i] > 0) {
                reqs.add(upsertRequest(i + 1));
            } else {
                reqs.add(deleteRequest(i + 1));
            }
        }
        return reqs;
    }

    public static List<UpsertRequest> upsertRequests(int qty) {
        List<UpsertRequest> reqs = new ArrayList<>();
        for (int i = 0; i < qty; i++) {
            reqs.add(upsertRequest(i + 1));
        }
        return reqs;
    }
    public static UpsertRequest upsertRequest(int index) {
        Properties meta = new Properties();
        meta.add("title", "Sample document " + index);
        UpsertRequest req = new UpsertRequest(
                "http://example.com/page" + index + ".html", meta,
                IOUtils.toInputStream(
                        "This is fake content for sample document " + index,
                        StandardCharsets.UTF_8));
        return req;
    }

    public static List<DeleteRequest> deleteRequests(int qty) {
        List<DeleteRequest> reqs = new ArrayList<>();
        for (int i = 0; i < qty; i++) {
            reqs.add(deleteRequest(i + 1));
        }
        return reqs;
    }
    public static DeleteRequest deleteRequest(int index) {
        Properties meta = new Properties();
        meta.add("title", "Sample document " + index);
        DeleteRequest req = new DeleteRequest(
                "http://example.com/page" + index + ".html", meta);
        return req;
    }
}
