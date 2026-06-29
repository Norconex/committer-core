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
package com.norconex.committer.core3;

/**
 * Commits documents to their final destination (e.g. search engine).
 * Implementors are encouraged to use one of the abstract committer classes,
 * which handles event handling and common use cases.
 * 
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public interface ICommitter extends AutoCloseable {

    /**
     * Initializes this committer with execution context.
     * 
     * @param committerContext committer execution context
     * @throws CommitterException initialization failure
     */
    void init(CommitterContext committerContext) throws CommitterException;

    /**
     * Determines whether a request should be committed.
     * 
     * @param request request to evaluate
     * @return {@code true} if the request is accepted
     * @throws CommitterException acceptance check failure
     */
    boolean accept(ICommitterRequest request) throws CommitterException;

    /**
     * Creates or updates a document in the target repository.
     * 
     * @param upsertRequest request to upsert
     * @throws CommitterException upsert failure
     */
    void upsert(UpsertRequest upsertRequest) throws CommitterException;

    /**
     * Deletes a document from the target repository.
     * 
     * @param deleteRequest request to delete
     * @throws CommitterException delete failure
     */
    void delete(DeleteRequest deleteRequest) throws CommitterException;

    /**
     * Closes this committer and releases resources.
     * 
     * @throws CommitterException close failure
     */
    @Override
    void close() throws CommitterException;

    /**
     * Cleans any persisted information (e.g. queue) to ensure next run will
     * start fresh.
     * Calling this method on Committers not persisting anything between
     * each execution will have no effect. This method does NOT delete anything
     * previously committed on the target repository.
     * 
     * @throws CommitterException something went wrong cleaning the committer.
     */
    void clean() throws CommitterException;
}
