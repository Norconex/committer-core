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
package com.norconex.committer.impl;

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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.ICommitter;
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
 *  &lt;committer class="com.norconex.committer.impl.MultipleCommitters"&gt;
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

    private static final long serialVersionUID = 4409999298134733358L;
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
//            File targetFile = new File(
//                    content.getAbsolutePath() + "-multicommit-" + i);
//            try {
//                FileUtils.copyFile(content, targetFile);
//            } catch (IOException e) {
//                throw new CommitterException(
//                        "Cannot copy document for multi-commit addition.  Ref: "
//                                + reference, e);
//            }
            committer.add(reference, cachedInputStream, metadata);
//            targetFile.delete();
        }
    }

    @Override
    public void remove(String reference, Properties metadata) {
        
        for (int i = 0; i < committers.size(); i++) {
            ICommitter committer = committers.get(i);
//            File targetFile = new File(
//                    document.getAbsolutePath() + "-multicommit-" + i);
//            try {
//                FileUtils.copyFile(document, targetFile);
//            } catch (IOException e) {
//                throw new CommitterException(
//                        "Cannot copy document for multi-commit addition.  Ref: "
//                                + reference + " File: " + document, e);
//            }
            committer.remove(reference, metadata);
//            targetFile.delete();
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
    
}
