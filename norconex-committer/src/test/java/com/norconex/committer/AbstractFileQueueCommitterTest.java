package com.norconex.committer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.norconex.committer.core.AbstractFileQueueCommitter;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.IDeleteOperation;
import com.norconex.commons.lang.map.Properties;

public class AbstractFileQueueCommitterTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    
    @Test
    public void testMultipleCommitThread() throws Exception {
        
        final AtomicInteger counter = new AtomicInteger();

        final AbstractFileQueueCommitter committer = 
                new AbstractFileQueueCommitter() {

            @Override
            protected void commitAddition(IAddOperation operation)
                    throws IOException {
                counter.incrementAndGet();
                operation.delete();
            }

            @Override
            protected void commitDeletion(IDeleteOperation operation)
                    throws IOException {
                counter.incrementAndGet();
                operation.delete();
            }

            @Override
            protected void commitComplete() {
            }
        };
        
        File queue = temp.newFolder();
        committer.setQueueDir(queue.getPath());
        // Use a bigger number to make sure the files are not 
        // committed while they are added.
        committer.setQueueSize(1000);

        // Queue 50 files for additions
        for (int i = 0; i < 50; i++) {
            Properties metadata = new Properties();
            committer.add(Integer.toString(i), 
                    IOUtils.toInputStream("hello world!"), metadata);
        }
        // Queue 50 files for deletions
        for (int i = 50; i < 100; i++) {
            Properties metadata = new Properties();
            committer.remove(Integer.toString(i), metadata);
        }
        
        ExecutorService pool = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        committer.commit();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
        
        // Each file should have been processed exactly once
        assertEquals(100, counter.intValue());
        
        // All files should have been processed
        Collection<File> files = FileUtils.listFiles(queue, null, true);
        assertTrue(files.isEmpty());
    }

}
