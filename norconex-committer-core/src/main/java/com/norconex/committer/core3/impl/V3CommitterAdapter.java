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

import com.norconex.committer.core3.CommitterContext;
import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.ICommitter;
import com.norconex.committer.core3.UpsertRequest;

/**
 * Adapter class to have version 2.x Committer classes work under
 * version 3.x, allowing for a smooth migration.
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public final class V3CommitterAdapter implements ICommitter {

    private final com.norconex.committer.core.ICommitter v2;

    private V3CommitterAdapter(
            com.norconex.committer.core.ICommitter v2Committer) {
        this.v2 = v2Committer;
    }

    // Return as is... offering 2 variations allows to wrap without
    // caring if version 2 or 3.
    public static final ICommitter adapt(
            com.norconex.committer.core3.ICommitter v3Committer) {
        return v3Committer;
    }
    public static final ICommitter adapt(
            com.norconex.committer.core.ICommitter v2Committer) {
        return new V3CommitterAdapter(v2Committer);
    }

    @Override
    public void init(CommitterContext committerContext)
            throws CommitterException {
        //NOOP
    }

    @Override
    public void upsert(UpsertRequest upsertRequest) throws CommitterException {
        v2.add(upsertRequest.getReference(),
                upsertRequest.getContent(), upsertRequest.getMetadata());
    }

    @Override
    public void delete(DeleteRequest deleteRequest) throws CommitterException {
        v2.remove(deleteRequest.getReference(), deleteRequest.getMetadata());
    }

    @Override
    public void close() throws CommitterException {
        v2.commit();
    }
}
