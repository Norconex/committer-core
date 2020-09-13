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
package com.norconex.committer.core3.fs.impl;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.ICommitterRequest;
import com.norconex.committer.core3.TestUtil;
import com.norconex.committer.core3.fs.impl.CSVFileCommitter.Column;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>CSV File Committer tests.</p>
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
class CSVFileCommitterTest  {

    @TempDir
    public Path folder;

    //TODO check that formatting is ok by comparing output to a predefined string.

    @Test
    void testMergedCSVFileCommitter()
            throws CommitterException, IOException {
        String expected =
                "type,URL,title,content\n"
              + "upsert,http://example.com/1,title 1,content 1\n"
              + "delete,http://example.com/2,title 2,\n"
              + "upsert,http://example.com/3,title 3,content 3\n";

        CSVFileCommitter c = commitSampleData(false);


        Collection<File> files = TestUtil.listFSFiles(c.getDirectory());
        Assertions.assertEquals(1,  files.size());

        String actual = FileUtils.readFileToString(
                files.iterator().next(), StandardCharsets.UTF_8);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testSplitCSVFileCommitter()
            throws CommitterException, IOException {

        String expectedUpsert =
                "URL,title,content\n"
              + "http://example.com/1,title 1,content 1\n"
              + "http://example.com/3,title 3,content 3\n";

        String expectedDelete =
                "URL,title,content\n"
              + "http://example.com/2,title 2,\n";

        CSVFileCommitter c = commitSampleData(true);

        Collection<File> files = TestUtil.listFSFiles(c.getDirectory());
        Assertions.assertEquals(2,  files.size());

        String actualUpsert = FileUtils.readFileToString(
                TestUtil.listFSUpsertFiles(
                        c.getDirectory()).iterator().next(), UTF_8);
        String actualDelete = FileUtils.readFileToString(
                TestUtil.listFSDeleteFiles(
                        c.getDirectory()).iterator().next(), UTF_8);

        Assertions.assertEquals(expectedUpsert, actualUpsert);
        Assertions.assertEquals(expectedDelete, actualDelete);
    }

    private CSVFileCommitter commitSampleData(
            boolean splitUpsertDelete) throws CommitterException {
        List<ICommitterRequest> reqs = new ArrayList<>();
        reqs.add(TestUtil.upsertRequest("http://example.com/1", "content 1",
                "type", "upsert",
                "document.reference", "http://example.com/1",
                "title", "title 1",
                "noise", "blah1"));
        reqs.add(TestUtil.deleteRequest("http://example.com/2",
                "type", "delete",
                "document.reference", "http://example.com/2",
                "title", "title 2",
                "noise", "blah2"));
        reqs.add(TestUtil.upsertRequest("http://example.com/3", "content 3",
                "type", "upsert",
                "document.reference", "http://example.com/3",
                "title", "title 3",
                "noise", "blah3"));

        CSVFileCommitter c = new CSVFileCommitter();
        // write 5 upserts and 2 deletes.
        // max docs per file being 2, so should generate 4 files.
        c.setDocsPerFile(20);
        c.setSplitUpsertDelete(splitUpsertDelete);
        c.setShowHeaders(true);

        c.setColumns(
                new Column("document.reference", "URL"),
                new Column("title"),
                new Column(null));

        c.init(TestUtil.committerContext(folder));
        TestUtil.commitRequests(c, reqs);
        c.close();
        return c;
    }

    @Test
    void testWriteRead() {
        CSVFileCommitter c = new CSVFileCommitter();
        c.setCompress(true);
        c.setDirectory(Paths.get("c:\\temp"));
        c.setDocsPerFile(5);
        c.setFileNamePrefix("prefix");
        c.setFileNamePrefix("suffix");
        c.setSplitUpsertDelete(true);
        c.setFormat("EXCEL");
        c.setDelimiter('|');
        c.setQuote('!');
        c.setShowHeaders(true);
        c.setEscape('%');
        c.setTruncateAt(10);
        c.setMultiValueJoinDelimiter("^^^");
        c.setTypeHeader("Request Type");
        c.setColumns(
                new Column("document.reference", "URL", 2000),
                new Column("title", "My Title", 100),
                new Column(null, "My Content", 200));
        XML.assertWriteRead(c, "committer");
    }

}
