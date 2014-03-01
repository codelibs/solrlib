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

package org.codelibs.solr.lib.server;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpSolrServer implementation.
 * 
 * @author shinsuke
 * 
 */
public class SolrLibHttpSolrServer extends HttpSolrServer {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory
            .getLogger(SolrLibHttpSolrServer.class);

    protected final HttpClientWrapper internalHttpClient;

    protected RequestConfig.Builder requestConfigBuilder = RequestConfig
            .custom();

    protected List<HttpRequestInterceptor> httpRequestInterceptorList = new ArrayList<HttpRequestInterceptor>();

    protected IdleConnectionMonitorThread idleConnectionMonitorThread;

    private boolean allowCompression;

    private boolean followRedirects = false;

    private int defaultMaxConnectionsPerHost = 32;

    private int maxTotalConnections = 128;

    private long connectionIdelTimeout = 60;

    private long connectionMonitorInterval = 10;

    private HttpClientConnectionManager clientConnectionManager;

    public SolrLibHttpSolrServer(final String solrServerUrl)
            throws MalformedURLException {
        this(solrServerUrl, new BinaryResponseParser());
    }

    public SolrLibHttpSolrServer(final String solrServerUrl,
            final ResponseParser parser) throws MalformedURLException {
        super(solrServerUrl, new HttpClientWrapper(), parser);
        internalHttpClient = (HttpClientWrapper) getHttpClient();
    }

    public void setCredentials(final AuthScope authScope,
            final Credentials credentials) {
        internalHttpClient.setCredentials(authScope, credentials);
    }

    public void addRequestInterceptor(final HttpRequestInterceptor iterceptor) {
        httpRequestInterceptorList.add(iterceptor);
    }

    @Override
    public void setConnectionTimeout(final int timeout) {
        requestConfigBuilder.setConnectTimeout(timeout);
    }

    @Override
    public void setSoTimeout(final int timeout) {
        requestConfigBuilder.setSocketTimeout(timeout);
    }

    @Override
    public void setFollowRedirects(final boolean followRedirects) {
        this.followRedirects = followRedirects;
    }

    @Override
    public void setAllowCompression(final boolean allowCompression) {
        this.allowCompression = allowCompression;
    }

    @Override
    public void setDefaultMaxConnectionsPerHost(
            final int defaultMaxConnectionsPerHost) {
        this.defaultMaxConnectionsPerHost = defaultMaxConnectionsPerHost;
    }

    @Override
    public void setMaxTotalConnections(final int maxTotalConnections) {
        this.maxTotalConnections = maxTotalConnections;
    }

    public void setConnectionIdelTimeout(final long connectionIdelTimeout) {
        this.connectionIdelTimeout = connectionIdelTimeout;
    }

    public void setConnectionMonitorInterval(
            final long connectionMonitorInterval) {
        this.connectionMonitorInterval = connectionMonitorInterval;
    }

    public void setClientConnectionManager(
            final HttpClientConnectionManager clientConnectionManager) {
        this.clientConnectionManager = clientConnectionManager;
    }

    // @InitMethod
    public void init() {
        if (clientConnectionManager == null) {
            final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
            connectionManager
                    .setDefaultMaxPerRoute(defaultMaxConnectionsPerHost);
            connectionManager.setMaxTotal(maxTotalConnections);
            idleConnectionMonitorThread = new IdleConnectionMonitorThread(
                    connectionManager, connectionMonitorInterval,
                    connectionIdelTimeout);
            idleConnectionMonitorThread.start();
            clientConnectionManager = connectionManager;
        }

        requestConfigBuilder.setRedirectsEnabled(followRedirects);

        final HttpClientBuilder builder = HttpClients.custom();

        if (allowCompression) {
            builder.addInterceptorLast(new UseCompressionRequestInterceptor());
            builder.addInterceptorLast(new UseCompressionResponseInterceptor());
        }

        for (final HttpRequestInterceptor iterceptor : httpRequestInterceptorList) {
            builder.addInterceptorLast(iterceptor);
        }

        init(builder.setConnectionManager(clientConnectionManager)
                .setDefaultRequestConfig(requestConfigBuilder.build()).build());

    }

    public void init(final CloseableHttpClient closeableHttpClient) {
        internalHttpClient.closeableHttpClient = closeableHttpClient;
    }

    @Override
    public void shutdown() {
        if (getHttpClient() instanceof CloseableHttpClient) {
            try {
                ((CloseableHttpClient) getHttpClient()).close();
            } catch (final IOException e) {
                logger.error("Failed to close HttpClient", e);
            }
        }
        if (idleConnectionMonitorThread != null) {
            idleConnectionMonitorThread.shutdown();
        }
    }

    protected static class HttpClientWrapper extends CloseableHttpClient {
        protected CloseableHttpClient closeableHttpClient;

        protected CredentialsProvider credsProvider;

        public void setCredentials(final AuthScope authScope,
                final Credentials credentials) {
            if (authScope != null & credentials != null) {
                credsProvider = new BasicCredentialsProvider();
                credsProvider.setCredentials(authScope, credentials);
            }
        }

        protected HttpContext updateHttpContext(final HttpContext context) {
            if (credsProvider == null) {
                return context;
            }

            HttpContext newContext;
            if (context == null) {
                newContext = HttpClientContext.create();
            } else {
                newContext = context;
            }
            newContext.setAttribute(HttpClientContext.CREDS_PROVIDER,
                    credsProvider);
            return newContext;
        }

        @Override
        public void close() throws IOException {
            closeableHttpClient.close();
        }

        @Override
        public CloseableHttpResponse execute(final HttpHost target,
                final HttpRequest request, final HttpContext context)
                throws IOException, ClientProtocolException {
            return closeableHttpClient.execute(target, request,
                    updateHttpContext(context));
        }

        @Override
        public CloseableHttpResponse execute(final HttpUriRequest request,
                final HttpContext context) throws IOException,
                ClientProtocolException {
            return closeableHttpClient.execute(request,
                    updateHttpContext(context));
        }

        @Override
        public CloseableHttpResponse execute(final HttpUriRequest request)
                throws IOException, ClientProtocolException {
            return execute(request, (HttpContext) null);
        }

        @Override
        public CloseableHttpResponse execute(final HttpHost target,
                final HttpRequest request) throws IOException,
                ClientProtocolException {
            return closeableHttpClient.execute(target, request,
                    (HttpContext) null);
        }

        @Override
        public <T> T execute(final HttpUriRequest request,
                final ResponseHandler<? extends T> responseHandler)
                throws IOException, ClientProtocolException {
            return execute(request, responseHandler, null);
        }

        @Override
        public <T> T execute(final HttpUriRequest request,
                final ResponseHandler<? extends T> responseHandler,
                final HttpContext context) throws IOException,
                ClientProtocolException {
            return closeableHttpClient.execute(request, responseHandler,
                    updateHttpContext(context));
        }

        @Override
        public <T> T execute(final HttpHost target, final HttpRequest request,
                final ResponseHandler<? extends T> responseHandler)
                throws IOException, ClientProtocolException {
            return execute(target, request, responseHandler, null);
        }

        @Override
        public <T> T execute(final HttpHost target, final HttpRequest request,
                final ResponseHandler<? extends T> responseHandler,
                final HttpContext context) throws IOException,
                ClientProtocolException {
            return closeableHttpClient.execute(target, request,
                    responseHandler, updateHttpContext(context));
        }

        @Override
        public String toString() {
            return closeableHttpClient.toString();
        }

        @Override
        public int hashCode() {
            return closeableHttpClient.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            return closeableHttpClient.equals(obj);
        }

        @SuppressWarnings("deprecation")
        @Override
        public HttpParams getParams() {
            return closeableHttpClient.getParams();
        }

        @SuppressWarnings("deprecation")
        @Override
        public ClientConnectionManager getConnectionManager() {
            return closeableHttpClient.getConnectionManager();
        }

        @Override
        protected CloseableHttpResponse doExecute(final HttpHost target,
                final HttpRequest request, final HttpContext context)
                throws IOException, ClientProtocolException {
            // no-op
            return null;
        }
    }

    protected static class IdleConnectionMonitorThread extends Thread {

        private final HttpClientConnectionManager connMgr;

        private volatile boolean shutdown;

        private final long monitorInterval;

        private final long idelTimeout;

        protected IdleConnectionMonitorThread(
                final HttpClientConnectionManager connMgr,
                final long monitorInterval, final long idelTimeout) {
            super("SolrLibConnMonitor");
            this.connMgr = connMgr;
            this.monitorInterval = monitorInterval * 1000;
            this.idelTimeout = idelTimeout;
        }

        @Override
        public void run() {
            while (!shutdown) {
                try {
                    synchronized (this) {
                        wait(monitorInterval);
                        connMgr.closeExpiredConnections();
                        connMgr.closeIdleConnections(idelTimeout,
                                TimeUnit.SECONDS);
                    }
                } catch (final InterruptedException ex) {
                    // terminate
                }
            }
        }

        public void shutdown() {
            shutdown = true;
            synchronized (this) {
                notifyAll();
            }
        }

    }

    protected static class UseCompressionRequestInterceptor implements
            HttpRequestInterceptor {

        @Override
        public void process(final HttpRequest request, final HttpContext context)
                throws HttpException, IOException {
            if (!request.containsHeader("Accept-Encoding")) {
                request.addHeader("Accept-Encoding", "gzip, deflate");
            }
        }
    }

    protected static class UseCompressionResponseInterceptor implements
            HttpResponseInterceptor {

        @Override
        public void process(final HttpResponse response,
                final HttpContext context) throws HttpException, IOException {

            final HttpEntity entity = response.getEntity();
            final Header ceheader = entity.getContentEncoding();
            if (ceheader != null) {
                final HeaderElement[] codecs = ceheader.getElements();
                for (final HeaderElement codec : codecs) {
                    if (codec.getName().equalsIgnoreCase("gzip")) {
                        response.setEntity(new GzipDecompressingEntity(response
                                .getEntity()));
                        return;
                    }
                    if (codec.getName().equalsIgnoreCase("deflate")) {
                        response.setEntity(new DeflateDecompressingEntity(
                                response.getEntity()));
                        return;
                    }
                }
            }
        }

        private static class GzipDecompressingEntity extends HttpEntityWrapper {
            public GzipDecompressingEntity(final HttpEntity entity) {
                super(entity);
            }

            @Override
            public InputStream getContent() throws IOException,
                    IllegalStateException {
                return new GZIPInputStream(wrappedEntity.getContent());
            }

            @Override
            public long getContentLength() {
                return -1;
            }
        }

        private static class DeflateDecompressingEntity extends
                GzipDecompressingEntity {
            public DeflateDecompressingEntity(final HttpEntity entity) {
                super(entity);
            }

            @Override
            public InputStream getContent() throws IOException,
                    IllegalStateException {
                return new InflaterInputStream(wrappedEntity.getContent());
            }
        }
    }
}
