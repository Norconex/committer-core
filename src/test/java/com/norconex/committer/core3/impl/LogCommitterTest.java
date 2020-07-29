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

import org.junit.jupiter.api.Test;

import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.TestUtil;
import com.norconex.commons.lang.text.TextMatcher;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>LogCommitter tests.</p>
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public class LogCommitterTest  {

    @Test
    public void testMemoryCommitter() throws CommitterException {
        // write 5 upserts and 2 deletes.
        LogCommitter c = new LogCommitter();

        c.init(TestUtil.committerContext(null));
        TestUtil.commitRequests(c, TestUtil.mixedRequests(1, 0, 1, 1, 1, 0, 1));
        c.close();

        // No tests, should simply not fail.
    }

    @Test
    public void testWriteRead() {
        LogCommitter c = new LogCommitter();
        c.setFieldMatcher(TextMatcher.wildcard("pot?to"));
        c.setIgnoreContent(true);
        c.setLogLevel("ERROR");
        XML.assertWriteRead(c, "committer");
    }
}
