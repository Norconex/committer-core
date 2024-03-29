/* Copyright 2021 Norconex Inc.
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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.norconex.commons.lang.xml.ErrorHandlerCapturer;
import com.norconex.commons.lang.xml.XML;

class CommitterTest {
    @Test
    public void testValidation() throws IOException {
        try (Reader r = new InputStreamReader(getClass().getResourceAsStream(
                "/validation/committer-core-full.xml"))) {
            ErrorHandlerCapturer eh = new ErrorHandlerCapturer();
            XML xml = XML.of(r).setErrorHandler(eh).create();

            List<ICommitter> committers = xml.getObjectListImpl(
                    ICommitter.class,
                    "/committers/committer",
                    Collections.emptyList());

            assertEquals(5, committers.size());
            assertEquals(0, eh.getErrors().size(),
                "Validation warnings/errors were found: " + eh.getErrors());
        }
    }
}
