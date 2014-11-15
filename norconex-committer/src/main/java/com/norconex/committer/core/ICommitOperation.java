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

import java.io.Serializable;

/**
 * Implementations represent the different types of commit operations that
 * can take place on a remote repository, and hold all necessary information
 * for a successful commit.
 * <p/>
 * {@link IAddOperation} and {@link IDeleteOperation} are the two types of 
 * operations typically used by most repositories.
 * 
 * @author Pascal Essiembre
 * @since 1.1.0
 */
public interface ICommitOperation extends Serializable {

    /**
     * Deletes the operation.  Typically removes it from
     * a memory or filesystem queue, or others
     */
    void delete();
}
