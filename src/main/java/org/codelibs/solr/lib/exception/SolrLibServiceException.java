/*
 * Copyright 2012-2012 the CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package org.codelibs.solr.lib.exception;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.codelibs.core.util.StringUtil;

public class SolrLibServiceException extends SolrLibException {

    private static final long serialVersionUID = 1L;

    protected final List<Exception> exceptionList = new ArrayList<Exception>();

    public SolrLibServiceException(final String messageCode, final Object[] args) {
        super(messageCode, args);
    }

    public void addException(final Exception e) {
        exceptionList.add(e);
    }

    @Override
    public String getMessage() {
        final StringBuilder buf = new StringBuilder();
        buf.append(super.getMessage());
        buf.append(String.format("%nThis exception has %d child exceptions.",
                exceptionList.size()));
        int count = 1;
        for (final Exception e : exceptionList) {
            buf.append(StringUtil.RETURN_STRING);
            buf.append("===> Exception ");
            buf.append(count);
            buf.append(": ");
            buf.append(e.getMessage());
            buf.append(StringUtil.RETURN_STRING);
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            buf.append(sw.toString());
            count++;
        }
        buf.append(StringUtil.RETURN_STRING);
        buf.append("Root Exception:");
        return buf.toString();
    }
}
