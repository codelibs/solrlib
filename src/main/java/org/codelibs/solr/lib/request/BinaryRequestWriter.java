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

package org.codelibs.solr.lib.request;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.ContentStream;
import org.codelibs.solr.lib.exception.SolrLibException;

public class BinaryRequestWriter extends RequestWriter {

    @Override
    public Collection<ContentStream> getContentStreams(final SolrRequest req)
            throws IOException {
        if (req instanceof UpdateRequest) {
            final UpdateRequest updateRequest = (UpdateRequest) req;
            if (isNull(updateRequest.getDocuments())
                    && isNull(updateRequest.getDeleteById())
                    && isNull(updateRequest.getDeleteQuery())
                    && updateRequest.getDocIterator() == null) {
                return null;
            }
            final List<ContentStream> l = new ArrayList<ContentStream>();
            l.add(new DelegateContentStream(updateRequest));
            return l;
        } else {
            return super.getContentStreams(req);
        }

    }

    @Override
    public String getUpdateContentType() {
        return "application/octet-stream";
    }

    @Override
    public ContentStream getContentStream(final UpdateRequest request)
            throws IOException {
        final BAOS baos = new BAOS();
        new JavaBinUpdateRequestCodec().marshal(request, baos);
        return new ContentStream() {
            @Override
            public String getName() {
                return null;
            }

            @Override
            public String getSourceInfo() {
                return "javabin";
            }

            @Override
            public String getContentType() {
                return "application/octet-stream";
            }

            @Override
            public Long getSize() // size if we know it, otherwise null
            {
                return Long.valueOf(baos.size());
            }

            @Override
            public InputStream getStream() throws IOException {
                return new ByteArrayInputStream(baos.getbuf(), 0, baos.size());
            }

            @Override
            public Reader getReader() throws IOException {
                throw new SolrLibException(
                        "No reader available . this is a binarystream");
            }
        };
    }

    @Override
    public void write(final SolrRequest request, final OutputStream os)
            throws IOException {
        if (request instanceof UpdateRequest) {
            final UpdateRequest updateRequest = (UpdateRequest) request;
            new JavaBinUpdateRequestCodec().marshal(updateRequest, os);
        }

    }/*
     * A hack to get access to the protected internal buffer and avoid an additional copy 
     */

    static class BAOS extends ByteArrayOutputStream {
        byte[] getbuf() {
            return super.buf;
        }
    }

    @Override
    public String getPath(final SolrRequest req) {
        if (req instanceof UpdateRequest) {
            return "/update/javabin";
        } else {
            return req.getPath();
        }
    }

    public class DelegateContentStream implements ContentStream {
        ContentStream contentStream = null;

        UpdateRequest req = null;

        public DelegateContentStream(final UpdateRequest req) {
            this.req = req;
        }

        private ContentStream getDelegate() {
            if (contentStream == null) {
                try {
                    contentStream = getContentStream(req);
                } catch (final IOException e) {
                    throw new SolrLibException(
                            "Unable to write xml into a stream", e);
                }
            }
            return contentStream;
        }

        @Override
        public String getName() {
            return getDelegate().getName();
        }

        @Override
        public String getSourceInfo() {
            return getDelegate().getSourceInfo();
        }

        @Override
        public String getContentType() {
            return getUpdateContentType();
        }

        @Override
        public Long getSize() {
            return getDelegate().getSize();
        }

        @Override
        public InputStream getStream() throws IOException {
            return getDelegate().getStream();
        }

        @Override
        public Reader getReader() throws IOException {
            return getDelegate().getReader();
        }

        public void writeTo(final OutputStream os) throws IOException {
            write(req, os);

        }
    }
}
