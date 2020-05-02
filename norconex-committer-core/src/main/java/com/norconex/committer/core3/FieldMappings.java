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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections4.map.UnmodifiableMap;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.norconex.commons.lang.xml.IXMLConfigurable;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>
 * A set of mappings that allow explicit setting of source and target
 * reference and content fields, as well as optional mappings for
 * any other fields.
 * </p>
 *
 * {@nx.xml.usage
 * <fieldMappings>
 *   <referenceField
 *       source="(leave out to use document reference)"
 *       target="(target field holding the document reference)"/>
 *   <contentField
 *       source="(leave out to use document content stream)"
 *       target="(target field holding the document "content")"/>
 *   <!-- Other fields to map. Else the same name is kept. Repeat as needed. -->
 *   <field
 *      source="(source field name"
 *      target="(target field name)"/>
 * </fieldMappings>
 * }
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public class FieldMappings implements IXMLConfigurable {

    private FieldMapping referenceMapping;
    private FieldMapping contentMapping;
    // Other fields
    private final Map<String, String> mappings = new HashMap<>();

    private final String defaultReferenceTarget;
    private final String defaultContentTarget;


    /**
     * Creates a new field mappings where reference and content target
     * field names are "id" and "content, respectively.
     */
    public FieldMappings() {
        this("id", "content");
    }
    public FieldMappings(String defaultReferenceTarget,
            String defaultContentTarget) {
        super();
        this.defaultReferenceTarget = defaultReferenceTarget;
        this.defaultContentTarget = defaultContentTarget;
    }

    public FieldMapping getReferenceMapping() {
        return referenceMapping;
    }
    public void setReferenceMapping(FieldMapping referenceMapping) {
        this.referenceMapping = referenceMapping;
    }

    public FieldMapping getContentMapping() {
        return contentMapping;
    }
    public void setContentMapping(FieldMapping contentMapping) {
        this.contentMapping = contentMapping;
    }

    public void addMapping(String source, String target) {
        mappings.put(source,  target);
    }
    public void addMappings(FieldMapping... mappings) {
        addMappings(new HashSet<>(Arrays.asList(mappings)));
    }
    public void addMappings(Set<FieldMapping> mappings) {
        for (FieldMapping mapping : mappings) {
            this.mappings.put(mapping.getSource(), mapping.getTarget());
        }
    }
    public void setMappings(Set<FieldMapping> mappings) {
        this.mappings.clear();
        addMappings(mappings);
    }
    public Map<String, String> getMappingsAsMap() {
        return UnmodifiableMap.unmodifiableMap(mappings);
    }
    public void setMappingsFromMap(Map<String, String> mappings) {
        mappings.clear();
        mappings.putAll(mappings);
    }
    public String getMappedTarget(String source) {
        return mappings.get(source);
    }
    public boolean isSourceMapped(String source) {
        return mappings.containsKey(source);
    }

    public String getDefaultContentTarget() {
        return defaultContentTarget;
    }
    public String getDefaultReferenceTarget() {
        return defaultReferenceTarget;
    }

    public boolean isEmpty() {
        return referenceMapping == null
                && contentMapping == null && mappings.isEmpty();
    }

    @Override
    public void loadFromXML(XML xml) {
        if (xml == null) {
            return;
        }
        referenceMapping = FieldMapping.loadFromXML(
                xml.getXML("referenceField"), referenceMapping);
        contentMapping = FieldMapping.loadFromXML(
                xml.getXML("contentField"), contentMapping);
        List<XML> xmlMappings = xml.getXMLList("field");
        for (XML xmlMapping : xmlMappings) {
            addMappings(FieldMapping.loadFromXML(xmlMapping, null));
        }
    }
    @Override
    public void saveToXML(XML xml) {
        if (xml == null) {
            return;
        }
        FieldMapping.saveToXML(
                xml.addElement("referenceField"), referenceMapping);
        FieldMapping.saveToXML(
                xml.addElement("contentField"), contentMapping);
        for (Entry<String, String> en : mappings.entrySet()) {
            FieldMapping.saveToXML(xml.addElement("field"),
                    new FieldMapping(en.getKey(), en.getValue()));
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
