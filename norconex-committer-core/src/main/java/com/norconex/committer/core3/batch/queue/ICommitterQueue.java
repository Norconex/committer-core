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
package com.norconex.committer.core3.batch.queue;

import com.norconex.committer.core3.CommitterContext;
import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.ICommitterRequest;
import com.norconex.committer.core3.batch.BatchConsumer;

/**
 * A committer queue, accumulating requests before they are committed
 * in one or multiple batches.
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public interface ICommitterQueue extends AutoCloseable {

    //TODO have abstract committer queue that takes care of initialization?
    void init(CommitterContext committerContext, BatchConsumer batchConsumer)
            throws CommitterException;

    //TODO Return new queue size after this queue request?
    void queue(ICommitterRequest request) throws CommitterException;

    @Override
    void close() throws CommitterException;
}
