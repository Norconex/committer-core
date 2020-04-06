/* Copyright 2010-2020 Norconex Inc.
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

/**
 * Triggered when something went wrong with committing.
 * @author Pascal Essiembre
 * @deprecated Since 3.0.0.
 */
@Deprecated
public class CommitterException extends RuntimeException {

    private static final long serialVersionUID = -805913995358009121L;

    /**
     * Constructor.
     */
    public CommitterException() {
        super();
    }
    /**
     * Constructor.
     * @param message error message
     */
    public CommitterException(String message) {
        super(message);
    }
    /**
     * Constructor.
     * @param cause original exception
     */
    public CommitterException(Throwable cause) {
        super(cause);
    }
    /**
     * Constructor.
     * @param message error message.
     * @param cause original exception
     */
    public CommitterException(String message, Throwable cause) {
        super(message, cause);
    }

}
