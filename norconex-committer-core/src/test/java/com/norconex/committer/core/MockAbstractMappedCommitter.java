package com.norconex.committer.core;

import java.util.ArrayList;
import java.util.List;

import com.norconex.commons.lang.xml.XML;

public class MockAbstractMappedCommitter extends AbstractMappedCommitter {

    private List<ICommitOperation> commitBatch;
    private boolean committed = false;

    @Override
    protected void commitBatch(List<ICommitOperation> batch) {
        commitBatch = new ArrayList<>(batch);
    }
    public List<ICommitOperation> getCommitBatch() {
        return commitBatch;
    }
    public boolean isCommitted() {
        return committed;
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
