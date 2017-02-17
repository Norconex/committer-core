package com.norconex.committer.core;

import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.XMLConfiguration;

public class MockAbstractMappedCommitter extends AbstractMappedCommitter {

    private List<ICommitOperation> commitBatch;
    private boolean committed = false;
    
    @Override
    protected void commitBatch(List<ICommitOperation> batch) {
        commitBatch = new ArrayList<ICommitOperation>(batch);
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
    protected void saveToXML(XMLStreamWriter writer)
            throws XMLStreamException {
        // no saving
    }
    @Override
    protected void loadFromXml(XMLConfiguration xml) {
        // no loading
    }

    
}
