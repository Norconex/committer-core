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

import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.event.Level;

import com.norconex.commons.lang.map.PropertyMatcher;
import com.norconex.commons.lang.map.PropertyMatchers;
import com.norconex.commons.lang.xml.IXMLConfigurable;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>
 * A base implementation taking care of basic plumbing, such as
 * firing main Committer events (including exceptions),
 * storing the Committer context (available via {@link #getCommitterContext()}),
 * and adding support for filtering unwanted requests.
 * </p>
 *
 * {@nx.xml.usage #restrictTo
 * <!-- multiple "restrictTo" tags allowed (only one needs to match) -->
 * <restrictTo>
 *   <fieldMatcher
 *     {@nx.include com.norconex.commons.lang.text.TextMatcher#matchAttributes}>
 *       (field-matching expression)
 *   </fieldMatcher>
 *   <valueMatcher
 *     {@nx.include com.norconex.commons.lang.text.TextMatcher#matchAttributes}>
 *       (value-matching expression)
 *   </valueMatcher>
 * </restrictTo>
 * }
 * <p>
 * Implementing classes inherit the above XML configuration.
 * </p>
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
@SuppressWarnings("javadoc")
public abstract class AbstractCommitter
        implements ICommitter, IXMLConfigurable {

    private CommitterContext committerContext;
    private final PropertyMatchers restrictions = new PropertyMatchers();

    /**
     * Adds one or more restrictions this committer should be restricted to.
     * @param restrictions the restrictions
     */
    public void addRestriction(PropertyMatcher... restrictions) {
        this.restrictions.addAll(restrictions);
    }
    /**
     * Adds restrictions this committer should be restricted to.
     * @param restrictions the restrictions
     */
    public void addRestrictions(
            List<PropertyMatcher> restrictions) {
        if (restrictions != null) {
            this.restrictions.addAll(restrictions);
        }
    }
    /**
     * Removes all restrictions on a given field.
     * @param field the field to remove restrictions on
     * @return how many elements were removed
     */
    public int removeRestriction(String field) {
        return restrictions.remove(field);
    }
    /**
     * Removes a restriction.
     * @param restriction the restriction to remove
     * @return <code>true</code> if this committer contained the restriction
     */
    public boolean removeRestriction(PropertyMatcher restriction) {
        return restrictions.remove(restriction);
    }
    /**
     * Clears all restrictions.
     */
    public void clearRestrictions() {
        restrictions.clear();
    }

    /**
     * Gets all restrictions
     * @return the restrictions
     * @since 2.4.0
     */
    public PropertyMatchers getRestrictions() {
        return restrictions;
    }

    @Override
    public final void init(
            CommitterContext committerContext) throws CommitterException {
        this.committerContext = Objects.requireNonNull(
                committerContext, "'committerContext' must not be null.");
        fireInfo(CommitterEvent.COMMITTER_INIT_BEGIN);
        try {
            doInit();
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_INIT_ERROR, e);
            throw e;
        }
        fireInfo(CommitterEvent.COMMITTER_INIT_END);
    }

    @Override
    public boolean accept(ICommitterRequest request) throws CommitterException {
        try {
            if (restrictions.isEmpty()
                    || restrictions.matches(request.getMetadata())) {
                fireInfo(CommitterEvent.COMMITTER_ACCEPT_YES);
                return true;
            }
            fireInfo(CommitterEvent.COMMITTER_ACCEPT_NO);
        } catch (RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_ACCEPT_ERROR, e);
            throw e;
        }
        return false;
    }

    @Override
    public final void upsert(
            UpsertRequest upsertRequest) throws CommitterException {
        fireInfo(CommitterEvent.COMMITTER_UPSERT_BEGIN, upsertRequest);
        try {
            doUpsert(upsertRequest);
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_UPSERT_ERROR, upsertRequest, e);
            throw e;
        }
        fireInfo(CommitterEvent.COMMITTER_UPSERT_END, upsertRequest);
    }
    @Override
    public final void delete(
            DeleteRequest deleteRequest) throws CommitterException {
        fireInfo(CommitterEvent.COMMITTER_DELETE_BEGIN, deleteRequest);
        try {
            doDelete(deleteRequest);
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_DELETE_ERROR, deleteRequest, e);
            throw e;
        }
        fireInfo(CommitterEvent.COMMITTER_DELETE_END, deleteRequest);
    }

    @Override
    public final void close() throws CommitterException {
        fireInfo(CommitterEvent.COMMITTER_CLOSE_BEGIN);
        try {
            doClose();
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_CLOSE_ERROR, e);
            throw e;
        }
        fireInfo(CommitterEvent.COMMITTER_CLOSE_END);
    }

    @Override
    public final void clean() throws CommitterException {
        fireInfo(CommitterEvent.COMMITTER_CLEAN_BEGIN);
        try {
            doClean();
        } catch (CommitterException | RuntimeException e) {
            fireError(CommitterEvent.COMMITTER_CLEAN_ERROR, e);
            throw e;
        }
        fireInfo(CommitterEvent.COMMITTER_CLEAN_END);
    }


    protected CommitterContext getCommitterContext() {
        return this.committerContext;
    }

    /**
     * Subclasses can perform additional initialization by overriding this
     * method. Default implementation does nothing. The
     * {@link CommitterContext} will be initialized when invoking
     * {@link #getCommitterContext()}
     * @throws CommitterException error initializing
     */
    protected abstract void doInit()
            throws CommitterException;
    protected abstract void doUpsert(UpsertRequest upsertRequest)
            throws CommitterException;
    protected abstract void doDelete(DeleteRequest deleteRequest)
            throws CommitterException;
    /**
     * Subclasses can perform additional closing logic by overriding this
     * method. Default implementation does nothing.
     * @throws CommitterException error closing committer
     */
    protected abstract void doClose() throws CommitterException;

    protected abstract void doClean() throws CommitterException;

    protected final void fireDebug(String name) {
        fireInfo(name, null);
    }
    protected final void fireDebug(String name, ICommitterRequest req) {
        committerContext.getEventManager().fire(
                CommitterEvent.create(name, this, req), Level.DEBUG);
    }
    protected final void fireInfo(String name) {
        fireInfo(name, null);
    }
    protected final void fireInfo(String name, ICommitterRequest req) {
        committerContext.getEventManager().fire(
                CommitterEvent.create(name, this, req));
    }
    protected final void fireError(String name, Exception e) {
        fireError(name, null, e);
    }
    protected final void fireError(
            String name, ICommitterRequest req, Exception e) {
        committerContext.getEventManager().fire(
                CommitterEvent.create(name, this, req, e), Level.ERROR);
    }

    @Override
    public final void loadFromXML(XML xml) {
        loadCommitterFromXML(xml);
        List<XML> nodes = xml.getXMLList("restrictTo");
        if (!nodes.isEmpty()) {
            restrictions.clear();
            for (XML node : nodes) {
                node.checkDeprecated("@field", "fieldMatcher", true);
                restrictions.add(PropertyMatcher.loadFromXML(node));
            }
        }
    }
    @Override
    public final void saveToXML(XML xml) {
        saveCommitterToXML(xml);
        restrictions.forEach(pm -> {
            PropertyMatcher.saveToXML(xml.addElement("restrictTo"), pm);
        });
    }

    public abstract void loadCommitterFromXML(XML xml);
    public abstract void saveCommitterToXML(XML xml);

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