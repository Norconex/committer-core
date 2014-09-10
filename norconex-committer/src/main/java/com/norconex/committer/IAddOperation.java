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
package com.norconex.committer;

import java.io.IOException;
import java.io.InputStream;

import com.norconex.commons.lang.map.Properties;

/**
 * Operation representing a new or updated document to be added to the
 * target repository.
 * @author Pascal Essiembre
 * @since 1.1.0
 */
public interface IAddOperation extends ICommitOperation {

    /**
     * Gets the document reference.
     * @return document reference
     */
    String getReference();
    
    /**
     * Gets the metadata.
     * @return metadata
     */
    Properties getMetadata();

    /**
     * Gets the content as a stream
     * @return content stream
     * @throws IOException
     */
    InputStream getContentStream() throws IOException;

}
