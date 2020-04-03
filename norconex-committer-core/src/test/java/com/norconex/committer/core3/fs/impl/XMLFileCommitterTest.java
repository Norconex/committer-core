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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.TestUtil;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>XML File Committer tests.</p>
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public class XMLFileCommitterTest  {

    @TempDir
    public Path folder;

    //TODO if all tests are same, create parameterized tests for FS committers.

    @Test
    public void testMergedXMLFileCommitter() throws CommitterException {
        // write 5 upserts and 2 deletes.
        // max docs per file being 2, so should generate 4 files.
        XMLFileCommitter c = new XMLFileCommitter();
        c.setDocsPerFile(2);
        c.setIndent(2);
        c.setSplitUpsertDelete(false);


        c.init(TestUtil.committerContext(folder));
        TestUtil.commitRequests(c, TestUtil.mixedRequests(1, 0, 1, 1, 1, 0, 1));
        c.close();

        assertEquals(4,  TestUtil.listFSFiles(c.getDirectory()).size());
        assertEquals(0,  TestUtil.listFSUpsertFiles(c.getDirectory()).size());
        assertEquals(0,  TestUtil.listFSDeleteFiles(c.getDirectory()).size());
    }

    @Test
    public void testSplitXMLFileCommitter() throws CommitterException {
        // write 5 upserts and 2 deletes.
        // max docs per file being 2, so should generate 3 upsert files
        // and 1 delete file
        XMLFileCommitter c = new XMLFileCommitter();
        c.setDocsPerFile(2);
        c.setIndent(2);
        c.setSplitUpsertDelete(true);

        c.init(TestUtil.committerContext(folder));
        TestUtil.commitRequests(c, TestUtil.mixedRequests(1, 0, 1, 1, 1, 0, 1));
        c.close();

        assertEquals(4,  TestUtil.listFSFiles(c.getDirectory()).size());
        assertEquals(3,  TestUtil.listFSUpsertFiles(c.getDirectory()).size());
        assertEquals(1,  TestUtil.listFSDeleteFiles(c.getDirectory()).size());
    }

    @Test
    public void testWriteRead() {
        XMLFileCommitter c = new XMLFileCommitter();
        c.setCompress(true);
        c.setDirectory(Paths.get("c:\\temp"));
        c.setDocsPerFile(5);
        c.setFileNamePrefix("prefix");
        c.setFileNamePrefix("suffix");
        c.setIndent(4);
        c.setSplitUpsertDelete(true);
        XML.assertWriteRead(c, "committer");
    }
}
