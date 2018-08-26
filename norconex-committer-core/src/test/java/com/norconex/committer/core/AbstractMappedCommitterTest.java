/* Copyright 2010-2018 Norconex Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.input.NullInputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.xml.XML;

/**
 * @author Pascal Essiembre
 */
public class AbstractMappedCommitterTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private StubCommitter committer;
    private boolean committed;

    private final Properties metadata = new Properties();

    private final String defaultReference = "1";

    /**
     * Sets up a committer for testing.
     * @throws IOException problem setting up committer
     */
    @Before
    public void setup() throws IOException {
        committer = new StubCommitter();
        File queue = tempFolder.newFolder("queue");
        committer.setQueueDir(queue.toString());

        committed = false;
        metadata.clear();
        metadata.add("myreference", defaultReference);
    }

    /**
     * Test no commit if not enough document
     * @throws IOException could not create temporary file
     */
    @Test
    public void testNoCommit() throws IOException {
        // Default batch size is 1000, so no commit should occur
        committer.add(defaultReference, new NullInputStream(0), metadata);
        assertFalse(committed);
    }

    /**
     * Test commit if there is enough document.
     * @throws IOException could not create temporary file
     */
    @Test
    public void testCommit() throws IOException {
        committer.setQueueSize(1);
        committer.add(defaultReference, new NullInputStream(0), metadata);
        assertTrue(committed);
    }

    /**
     * Test setting the source and target IDs.
     * @throws IOException could not create temporary file
     */
    @Test
    public void testSetSourceAndTargetReference() throws IOException {

        // Set a different source and target id
        String customSourceId = "mysourceid";
        committer.setSourceReferenceField(customSourceId);
        String customTargetId = "mytargetid";
        committer.setTargetReferenceField(customTargetId);

        // Store the source id value in metadata
        metadata.add(customSourceId, defaultReference);

        // Add a doc (it should trigger a commit because batch size is 1)
        committer.setQueueSize(1);
        committer.add(defaultReference, new NullInputStream(0), metadata);

        // Get the map generated
        assertEquals(1, committer.getCommitBatch().size());
        IAddOperation op = (IAddOperation) committer.getCommitBatch().get(0);
        Properties docMeta = op.getMetadata();

        // Check that customTargetId was used
        assertEquals(defaultReference, docMeta.getString(customTargetId));

        // Check that customSourceId was removed (default behavior)
        assertFalse("Source reference field was not removed.",
                docMeta.containsKey(customSourceId));
    }

    /**
     * Test keeping source id field.
     * @throws IOException could not create temporary file
     */
    @Test
    public void testKeepSourceId() throws IOException {

        committer.setKeepSourceReferenceField(true);

        // Add a doc (it should trigger a commit because batch size is 1)
        committer.setQueueSize(1);
        committer.add(defaultReference, new NullInputStream(0), metadata);
        committer.commit();

        // Get the map generated
        assertEquals(1, committer.getCommitBatch().size());
        IAddOperation op = (IAddOperation) committer.getCommitBatch().get(0);

        // Check that the source id is still there
        assertTrue(op.getMetadata().containsKey("myreference"));
    }

    class StubCommitter extends AbstractMappedCommitter {

        private List<ICommitOperation> commitBatch;

        @Override
        protected void commitBatch(List<ICommitOperation> batch) {
            commitBatch = new ArrayList<>(batch);
        }
        /**
         * @return the operationCount
         */
        public List<ICommitOperation> getCommitBatch() {
            return commitBatch;
        }
        @Override
        protected void commitComplete() {
            super.commitComplete();
            committed = true;
        }
        @Override
        protected void loadMappedCommitterFromXML(XML xml) {
            // NOOP
        }
        @Override
        protected void saveMappedCommitterToXML(XML xml) {
            // NOOP
        }
    }
}
