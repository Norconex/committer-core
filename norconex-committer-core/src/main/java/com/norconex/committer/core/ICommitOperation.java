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

import java.io.Serializable;

/**
 * Implementations represent the different types of commit operations that
 * can take place on a remote repository, and hold all necessary information
 * for a successful commit.
 * <br><br>
 * {@link IAddOperation} and {@link IDeleteOperation} are the two types of
 * operations typically used by most repositories.
 *
 * @author Pascal Essiembre
 * @since 1.1.0
 * @deprecated Since 3.0.0.
 */
@Deprecated
public interface ICommitOperation extends Serializable {

    /**
     * Deletes the operation.  Typically removes it from
     * a memory or filesystem queue, or others
     */
    void delete();
}
