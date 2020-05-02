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

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.norconex.commons.lang.xml.XML;

/**
 * <p>
 * A mapping between a source field name and target field name.
 * </p>
 * {@nx.xml.usage #attributes
 * source="(source field name)"
 * target="(target field name)"
 * }
 * <P>
 * The above is the recommended attribute for consuming classes to use
 * in XML configuration.
 * </p>
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public class FieldMapping implements Serializable {

    private static final long serialVersionUID = 7604565082134512199L;

    public static final FieldMapping EMPTY_FIELD_MAPPING =
            new FieldMapping(null, null);

    private final String source;
    private final String target;

    /**
     * Copy constructor.
     * @param fieldMapping mapping to copy
     */
    public FieldMapping(FieldMapping fieldMapping) {
        super();
        this.source = fieldMapping.source;
        this.target = fieldMapping.target;
    }
    public FieldMapping(String source, String target) {
        super();
        this.source = source;
        this.target = target;
    }

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public static FieldMapping loadFromXML(XML xml, FieldMapping defaultValue) {
        if (xml == null) {
            return null;
        }
        return new FieldMapping(
                xml.getString("@source", defaultValue == null ? null
                        : defaultValue.getSource()),
                xml.getString("@target", defaultValue == null ? null
                        : defaultValue.getTarget()));
    }
    public static void saveToXML(XML xml, FieldMapping fieldMapping) {
        if (fieldMapping != null) {
            xml.setAttribute("source", fieldMapping.getSource());
            xml.setAttribute("target", fieldMapping.getTarget());
        }
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
