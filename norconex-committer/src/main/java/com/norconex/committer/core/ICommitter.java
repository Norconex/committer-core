/* Copyright 2010-2014 Norconex Inc.
 * 
 * This file is part of Norconex Committer.
 * 
 * Norconex Committer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * Norconex Committer is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Norconex Committer. If not, see <http://www.gnu.org/licenses/>.
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
