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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Committer-related utility methods.
 * </p>
 *
 * @author Pascal Essiembre
 * @since 3.0.0
 */
public final class CommitterUtil {

    private static final Logger LOG =
            LoggerFactory.getLogger(CommitterUtil.class);

    private CommitterUtil() {
        super();
    }

    /**
     * Gets the request content as a string. Only applicable to
     * {@link UpsertRequest} instances.
     * @param req committer request
     * @return content as string, or <code>null</code>
     * @throws CommitterException could not get content as string
     */
    public static String getContentAsString(ICommitterRequest req)
            throws CommitterException {
        if (req instanceof UpsertRequest) {
            try {
                return IOUtils.toString(
                        ((UpsertRequest) req).getContent(), UTF_8);
            } catch (IOException e) {
                throw new CommitterException(
                        "Could not load document content for : "
                                + req.getReference(), e);
            }
        }
        return null;
    }

    /**
     * Extracts the source ID value. If the supplied source ID value
     * argument is not blank, the value is obtained from the corresponding
     * metadata field and that field is deleted. Otherwise the document
     * reference is returned.
     * @param req committer request
     * @param sourceIdField name of the metadata field holding the ID value
     *     or blank.
     * @return the source id value
     */
    public static String extractSourceIdValue(
            ICommitterRequest req, String sourceIdField) {
       return extractSourceIdValue(req, sourceIdField, false);
    }

    /**
     * Extracts the source ID value. If the supplied source ID value
     * argument is not blank, the value is obtained from the corresponding
     * metadata field. Otherwise the document reference is returned.
     * @param req committer request
     * @param sourceIdField name of the metadata field holding the ID value
     *     or blank.
     * @param keepSourceIdField whether to keep the source ID field
     * @return the source id value
     */
    public static String extractSourceIdValue(
            ICommitterRequest req,
            String sourceIdField,
            boolean keepSourceIdField) {
        String idValue = null;
        if (StringUtils.isNotBlank(sourceIdField)) {
            idValue = req.getMetadata().getString(sourceIdField);
            if (StringUtils.isNotBlank(idValue)) {
                if (!keepSourceIdField) {
                    // remove since remapped
                    req.getMetadata().remove(sourceIdField);
                }
            } else {
                LOG.warn("Source ID field \"{}\" has no value. "
                        + "Falling back to using document reference: {}",
                        sourceIdField, req.getReference());
            }
        }
        if (StringUtils.isBlank(idValue)) {
            idValue = req.getReference();
        }
        return idValue;
    }

    /**
     * Applies the document content (input stream) to the target
     * field name. If the <code>targetContentField</code> argument is
     * <code>null</code> or blank, the document content is ignored (not set).
     * Only applicable to {@link UpsertRequest} instances.
     * @param req committer request
     * @param targetContentField name of the target field holding the document
     *     content.
     * @throws CommitterException could not apply target content
     */
    public static void applyTargetContent(
            ICommitterRequest req, String targetContentField)
                    throws CommitterException {
        if (req instanceof UpsertRequest
                && StringUtils.isNotBlank(targetContentField)) {
            req.getMetadata().set(targetContentField, getContentAsString(req));
        }
    }

    /**
     * Applies the source ID field value after extracting it using
     * {@link #extractSourceIdValue(ICommitterRequest, String)},
     * to the target ID ID field supplied.
     * If <code>targetIdField</code> or source ID value, is <code>null</code>
     * or blank, the document ID is ignored (not set).
     * @param req committer request
     * @param sourceIdField name of the source field holding the document ID
     * @param targetIdField name of the target field holding the document ID
     */
    public static void applyTargetId(
            ICommitterRequest req, String sourceIdField, String targetIdField) {
        String sourceIdValue = extractSourceIdValue(req, sourceIdField);
        if (StringUtils.isNoneBlank(targetIdField, sourceIdValue)) {
            req.getMetadata().set(targetIdField, sourceIdValue);
        }
    }
}