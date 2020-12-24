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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.norconex.committer.core3.CommitterContext;
import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.ICommitterRequest;
import com.norconex.committer.core3.TestUtil;
import com.norconex.commons.lang.xml.XML;


/**
 * @author Pascal Essiembre
 */
class FSQueueTest {

    @TempDir
    static Path folder;

    private CommitterContext ctx;
    private FSQueue queue;

    @BeforeEach
    public void setup() {
        this.ctx = CommitterContext.builder().setWorkDir(folder).build();
        this.queue = new FSQueue();
    }

    @Test
    void testQueue() throws CommitterException, IOException {

        final MutableInt batchQty = new MutableInt();
        final Set<String> batchRefs = new TreeSet<>();

        queue.setBatchSize(5);
        queue.init(ctx, it -> {
            batchQty.increment();
            while (it.hasNext()) {
                ICommitterRequest req = it.next();
                batchRefs.add(req.getReference());
            }
        });

        // Add test data
        for (int i = 0; i < 13; i++) {
            queue.queue(TestUtil.upsertRequest(i + 1));
        }
        queue.close();

        // records should have been processed in 3 batches.
        Assertions.assertEquals(3, batchQty.getValue());

        // There should be 13 obtained from queue in total
        Assertions.assertEquals(13, batchRefs.size());

        // Queue directory should be empty.
        Assertions.assertEquals(0, Files.find(folder,  1,
                (f, a) -> f.toFile().getName().endsWith(
                        FSQueueUtil.EXT)).count());
    }

    @Test
    void testWriteRead() {
        FSQueue q = new FSQueue();
        q.setBatchSize(50);
        q.setMaxPerFolder(100);
        q.setCommitLeftoversOnInit(true);
        q.setIgnoreErrors(true);
        q.setMaxRetries(6);
        q.setRetryDelay(666);
        XML.assertWriteRead(q, "queue");
    }
}
