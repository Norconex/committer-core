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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.norconex.commons.lang.map.Properties;

/**
 * A committer deletion request. Metadata associated with a deletion
 * request is typically minimal in comparison with an addition or update.
 * It is even possible for it to be empty.
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public class DeleteRequest implements ICommitterRequest {

    private final String reference;
    private final Properties metadata = new Properties();

    public DeleteRequest(String reference, Properties metadata) {
        super();
        this.reference = reference;
        this.metadata.putAll(metadata);
    }

    @Override
    public String getReference() {
        return reference;
    }

    @Override
    public Properties getMetadata() {
        return metadata;
    }

    @Override
    public boolean equals(final Object other) {
        return EqualsBuilder.reflectionEquals(this, other);
    }
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
    @Override
    public String toString() {
        return new ReflectionToStringBuilder(
                this, ToStringStyle.SHORT_PREFIX_STYLE).toString();
    }
}
