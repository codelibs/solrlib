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

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;

@SuppressWarnings("deprecation")
public class SolrLibCommonsHttpSolrServer extends CommonsHttpSolrServer {

    private static final long serialVersionUID = 1L;

    public SolrLibCommonsHttpSolrServer(final String solrServerUrl)
            throws MalformedURLException {
        super(new URL(solrServerUrl));
    }

    public SolrLibCommonsHttpSolrServer(final String solrServerUrl,
            final HttpClient httpClient) throws MalformedURLException {
        super(new URL(solrServerUrl), httpClient, new BinaryResponseParser(),
                false);
    }

    public SolrLibCommonsHttpSolrServer(final String solrServerUrl,
            final HttpClient httpClient, final boolean useMultiPartPost)
            throws MalformedURLException {
        super(new URL(solrServerUrl), httpClient, new BinaryResponseParser(),
                useMultiPartPost);
    }

    public SolrLibCommonsHttpSolrServer(final String solrServerUrl,
            final HttpClient httpClient, final ResponseParser parser)
            throws MalformedURLException {
        super(new URL(solrServerUrl), httpClient, parser, false);
    }

    public SolrLibCommonsHttpSolrServer(final URL baseURL) {
        super(baseURL, null, new BinaryResponseParser(), false);
    }

    public SolrLibCommonsHttpSolrServer(final URL baseURL,
            final HttpClient client) {
        super(baseURL, client, new BinaryResponseParser(), false);
    }

    public SolrLibCommonsHttpSolrServer(final URL baseURL,
            final HttpClient client, final boolean useMultiPartPost) {
        super(baseURL, client, new BinaryResponseParser(), useMultiPartPost);
    }

    public SolrLibCommonsHttpSolrServer(final URL baseURL,
            final HttpClient client, final ResponseParser parser,
            final boolean useMultiPartPost) {
        super(baseURL, client, parser, useMultiPartPost);
    }

    public void setCredentials(final AuthScope authscope,
            final Credentials credentials) {
        getHttpClient().getState().setCredentials(authscope, credentials);
    }

    public void setAuthenticationPreemptive(final boolean value) {
        getHttpClient().getParams().setAuthenticationPreemptive(value);
    }

}
