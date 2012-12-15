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

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

/**
 * HttpSolrServer implementation.
 * 
 * @author shinsuke
 * 
 */
public class SolrLibHttpSolrServer extends HttpSolrServer {

    private static final long serialVersionUID = 1L;

    public SolrLibHttpSolrServer(final String solrServerUrl)
            throws MalformedURLException {
        super(solrServerUrl);
    }

    public SolrLibHttpSolrServer(final String solrServerUrl,
            final HttpClient httpClient) throws MalformedURLException {
        super(solrServerUrl, httpClient, new BinaryResponseParser());
    }

    public SolrLibHttpSolrServer(final String solrServerUrl,
            final HttpClient httpClient, final ResponseParser parser)
            throws MalformedURLException {
        super(solrServerUrl, httpClient, parser);
    }

    public void setCredentials(final AuthScope authscope,
            final Credentials credentials) {
        final HttpClient httpClient = getHttpClient();
        if (httpClient instanceof AbstractHttpClient) {
            ((AbstractHttpClient) httpClient).getCredentialsProvider()
                    .setCredentials(authscope, credentials);
        }
    }

}
