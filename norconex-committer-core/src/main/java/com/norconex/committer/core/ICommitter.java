/* Copyright 2010-2014 Norconex Inc.
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

import java.io.InputStream;

import com.norconex.commons.lang.map.Properties;

/**
 * Commits documents to their final destination (e.g. search engine).
 * @author Pascal Essiembre
 */
@SuppressWarnings("nls")
public interface ICommitter {

    /**
     * Adds a new or modified document to the target destination.  
     * Implementations may decide to queue the addition request instead until 
     * commit is called, or a certain threshold is reached.
     * @param reference document reference (e.g. URL)
     * @param content document content
     * @param metadata document metadata
     * @since 2.0.0
     */
    void add(String reference, InputStream content, Properties metadata);

    /**
     * Removes a document from the target destination.  
     * Implementations may decide to queue the removal request instead until 
     * commit is called, or a certain threshold is reached.
     * @param reference document reference (e.g. URL)
     * @param metadata document metadata
     * @since 2.0.0
     */
    void remove(String reference, Properties metadata);
        
    /**
     * Commits documents.  Effectively apply the additions and removals.  
     * May not be necessary for some implementations.
     */
    void commit();
}
