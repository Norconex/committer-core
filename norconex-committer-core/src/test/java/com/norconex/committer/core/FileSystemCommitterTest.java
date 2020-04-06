/* Copyright 2010-2020 Norconex Inc.
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
import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.norconex.committer.core.impl.FileSystemCommitter;
import com.norconex.commons.lang.xml.XML;

/**
 * Tests the {@link FileSystemCommitter}.
 * @author Pascal Essiembre
 * @deprecated
 */
@Deprecated
public class FileSystemCommitterTest {

    private File tempFile;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = File.createTempFile("FileSystemCommitterTest", ".xml");
    }

    @AfterEach
    public void tearDown() throws Exception {
        tempFile.delete();
    }

    @Test
    public void testWriteRead() {
        FileSystemCommitter outCommitter = new FileSystemCommitter();
        outCommitter.setDirectory("C:\\FakeTestDirectory\\");
        XML.assertWriteRead(outCommitter, "committer");
    }

}
