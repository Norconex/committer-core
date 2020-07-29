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
package com.norconex.committer.core3.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.TestUtil;

/**
 * <p>MemoryCommitter tests.</p>
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public class MemoryCommitterTest  {

    @Test
    public void testMemoryCommitter() throws CommitterException {
        // write 5 upserts and 2 deletes.
        MemoryCommitter c = new MemoryCommitter();

        c.init(TestUtil.committerContext(null));
        TestUtil.commitRequests(c, TestUtil.mixedRequests(1, 0, 1, 1, 1, 0, 1));
        c.close();

        assertEquals(7,  c.getRequestCount());
        assertEquals(5,  c.getUpsertCount());
        assertEquals(2,  c.getDeleteCount());
    }
}
