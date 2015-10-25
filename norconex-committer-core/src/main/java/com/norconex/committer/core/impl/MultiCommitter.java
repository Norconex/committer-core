/* Copyright 2010-2015 Norconex Inc.
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
package com.norconex.committer.core.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.core.ICommitter;
import com.norconex.commons.lang.config.ConfigurationUtil;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.io.CachedInputStream;
import com.norconex.commons.lang.io.CachedStreamFactory;
import com.norconex.commons.lang.map.Properties;

/**
 * This committer allows you to define user many committers as one. 
 * Every committing requests will be dispatched and handled by all nested 
 * committers defined (in the order they were added).
 * <p>
 * XML configuration usage:
 * </p>
 * <pre>
 *  &lt;committer class="com.norconex.committer.core.impl.MultipleCommitters"&gt;
 *      &lt;committer class="(committer class)"&gt;
 *          (Commmitter-specific configuration here)
 *      &lt;/committer&gt;
 *      &lt;committer class="(committer class)"&gt;
 *          (Commmitter-specific configuration here)
 *      &lt;/committer&gt;
 *      ...
 *  &lt;/committer&gt;
 * </pre>
 * @author Pascal Essiembre
 * @since 1.2.0
 */
public class MultiCommitter implements ICommitter, IXMLConfigurable {

    private static final Logger LOG = 
            LogManager.getLogger(FileSystemCommitter.class);
    
    private final List<ICommitter> committers = new ArrayList<ICommitter>();

    /**
     * Constructor.
     */
    public MultiCommitter() {
        super();
    }
    /**
     * Constructor.
     * @param committers a list of committers
     */
    public MultiCommitter(List<ICommitter> committers) {
        this.committers.addAll(committers);
    }
    
    /**
     * Adds one or more committers.
     * @param committer committers
     */
    public void addCommitter(ICommitter... committer) {
        this.committers.addAll(Arrays.asList(committer));
    }
    /**
     * Removes one or more committers.
     * @param committer committers
     */
    public void removeCommitter(ICommitter... committer) {
        this.committers.removeAll(Arrays.asList(committer));
    }
    /**
     * Gets nested committers.
     * @return committers
     */
    public List<ICommitter> getCommitters() {
        return new ArrayList<ICommitter>(committers);
    }
    
    @Override
    public void add(
            String reference, InputStream content, Properties metadata) {
        CachedStreamFactory factory = new CachedStreamFactory(
                (int) FileUtils.ONE_MB, (int) FileUtils.ONE_MB);
        CachedInputStream cachedInputStream = factory.newInputStream(content);
        for (int i = 0; i < committers.size(); i++) {
            ICommitter committer = committers.get(i);
            committer.add(reference, cachedInputStream, metadata);
        }
    }

    @Override
    public void remove(String reference, Properties metadata) {
        for (int i = 0; i < committers.size(); i++) {
            ICommitter committer = committers.get(i);
            committer.remove(reference, metadata);
        }
    }

    @Override
    public void commit() {
        for (ICommitter committer : committers) {
            committer.commit();
        }
    }

    @Override
    public void loadFromXML(Reader in) throws IOException {
        XMLConfiguration xml = ConfigurationUtil.newXMLConfiguration(in);
        List<HierarchicalConfiguration> xmlCommitters = 
                xml.configurationsAt("committer");
        for (HierarchicalConfiguration xmlCommitter : xmlCommitters) {
            addCommitter(
                    (ICommitter) ConfigurationUtil.newInstance(xmlCommitter));
        }
    }

    @Override
    public void saveToXML(Writer out) throws IOException {
        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        try {
            XMLStreamWriter writer = factory.createXMLStreamWriter(out);
            writer.writeStartElement("committer");
            writer.writeAttribute("class", getClass().getCanonicalName());
            for (ICommitter committer : committers) {
                writer.flush();
                if (!(committer instanceof IXMLConfigurable)) {
                    LOG.error("Cannot save committer to XML as it does not "
                            + "implement IXMLConfigurable: " + committer);
                }
                ((IXMLConfigurable) committer).saveToXML(out);
            }
            writer.writeEndElement();
            writer.flush();
            writer.close();
        } catch (XMLStreamException e) {
            throw new IOException("Cannot save as XML.", e);
        }
    }    
    
    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof MultiCommitter)) {
            return false;
        }
        MultiCommitter castOther = (MultiCommitter) other;
        return new EqualsBuilder()
                .append(committers, castOther.committers)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(committers)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("committers", committers)
                .toString();
    }    
}
