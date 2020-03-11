/* Copyright 2017-2020 Norconex Inc.
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
import java.io.InputStreamReader;
import java.io.Reader;

import org.junit.Assert;
import org.junit.Test;

import com.norconex.commons.lang.xml.XML;


/**
 * @author Pascal Essiembre
 */
public class CommitterTest {

    @Test
    public void testWriteRead() {
        MockAbstractMappedCommitter c = new MockAbstractMappedCommitter();
        c.setCommitBatchSize(10);
        c.setKeepSourceContentField(true);
        c.setKeepSourceReferenceField(true);
        c.setMaxRetries(3);
        c.setQueueDir(new File("C:\temp").getAbsolutePath());
        c.setQueueSize(5);
        c.setSourceContentField("sourceContentField");
        c.setSourceReferenceField("sourceReferenceField");
        c.setTargetContentField("targetContentField");
        c.setTargetReferenceField("targetReferenceField");
        System.out.println("Writing/Reading this: " + c);
        XML.assertWriteRead(c, "committer");
    }

    @Test
    public void testValidation() throws IOException {
        try (Reader r = new InputStreamReader(getClass().getResourceAsStream(
                "/validation/committer-core-full.xml"))) {
            Assert.assertEquals("Validation warnings/errors were found.",
                    0, XML.of(r).create().validate().size());
        }
    }
}
