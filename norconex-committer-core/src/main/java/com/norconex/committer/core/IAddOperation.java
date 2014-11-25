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
