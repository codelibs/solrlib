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

package org.codelibs.solr.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.codelibs.solr.lib.exception.SolrLibException;
import org.codelibs.solr.lib.exception.SolrLibGroupNotAvailableException;
import org.codelibs.solr.lib.exception.SolrLibQueryException;
import org.codelibs.solr.lib.exception.SolrLibServerNotAvailableException;
import org.codelibs.solr.lib.policy.QueryType;
import org.codelibs.solr.lib.policy.StatusPolicy;
import org.seasar.util.log.Logger;

public class SolrGroup {

    private static final Logger logger = Logger.getLogger(SolrGroup.class);

    protected static final int MAX_LOAD_COUNT = Integer.MAX_VALUE / 2;

    protected Map<String, SolrServer> solrServerMap = new LinkedHashMap<String, SolrServer>();

    protected String groupName;

    protected ConcurrentHashMap<String, AtomicInteger> accessCountMap = new ConcurrentHashMap<String, AtomicInteger>();

    protected ConcurrentHashMap<String, AtomicInteger> errorCountMap = new ConcurrentHashMap<String, AtomicInteger>();

    protected StatusPolicy statusPolicy;

    public void addServer(final String name, final SolrServer solrServer) {
        solrServerMap.put(name, solrServer);
    }

    public Collection<UpdateResponse> add(
            final Collection<SolrInputDocument> docs) {
        // check this group status
        checkStatus(QueryType.ADD);

        return updateQueryCallback(QueryType.ADD,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.add(docs);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    public Collection<UpdateResponse> add(final SolrInputDocument doc) {
        // check this group status
        checkStatus(QueryType.ADD);

        return updateQueryCallback(QueryType.ADD,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.add(doc);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    public Collection<UpdateResponse> addBean(final Object obj) {
        // check this group status
        checkStatus(QueryType.ADD);

        return updateQueryCallback(QueryType.ADD,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.addBean(obj);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    public Collection<UpdateResponse> addBean(final Collection<?> beans) {
        // check this group status
        checkStatus(QueryType.ADD);

        return updateQueryCallback(QueryType.ADD,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.addBeans(beans);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    public Collection<UpdateResponse> commit() {
        // check this group status
        checkStatus(QueryType.COMMIT);

        return updateQueryCallback(QueryType.COMMIT,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.commit();
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    public Collection<UpdateResponse> commit(final boolean waitFlush,
            final boolean waitSearcher) {
        // check this group status
        checkStatus(QueryType.COMMIT);

        return updateQueryCallback(QueryType.COMMIT,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.commit(waitFlush, waitSearcher);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    public Collection<UpdateResponse> deleteByQuery(final String query) {
        // check this group status
        checkStatus(QueryType.DELETE);

        return updateQueryCallback(QueryType.DELETE,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.deleteByQuery(query);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    public Collection<UpdateResponse> deleteById(final String id) {
        // check this group status
        checkStatus(QueryType.DELETE);

        return updateQueryCallback(QueryType.DELETE,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.deleteById(id);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    public Collection<UpdateResponse> optimize() {
        // check this group status
        checkStatus(QueryType.OPTIMIZE);

        return updateQueryCallback(QueryType.OPTIMIZE,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.optimize();
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    public Collection<UpdateResponse> optimize(final boolean waitFlush,
            final boolean waitSearcher) {
        // check this group status
        checkStatus(QueryType.OPTIMIZE);

        return updateQueryCallback(QueryType.OPTIMIZE,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.optimize(waitFlush, waitSearcher);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    public Collection<UpdateResponse> optimize(final boolean waitFlush,
            final boolean waitSearcher, final int maxSegments) {
        // check this group status
        checkStatus(QueryType.OPTIMIZE);

        return updateQueryCallback(QueryType.OPTIMIZE,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.optimize(waitFlush, waitSearcher,
                                    maxSegments);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    public Collection<SolrPingResponse> ping() {
        // check this group status
        checkStatus(QueryType.PING);

        return updateQueryCallback(QueryType.PING,
                new UpdateProcessCallback<SolrPingResponse>() {
                    @Override
                    public SolrPingResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.ping();
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    public QueryResponse query(final SolrParams params) {
        return query(params, SolrRequest.METHOD.GET);
    }

    public QueryResponse query(final SolrParams params, final METHOD method) {
        // check this group status
        checkStatus(QueryType.QUERY);

        SolrLibQueryException fsqe = null;
        final int maxRetryCount = statusPolicy
                .getMaxRetryCount(QueryType.QUERY);
        for (int i = 0; i < maxRetryCount; i++) {
            try {
                return queryInternal(params, method);
            } catch (final Exception e) {
                if (fsqe == null) {
                    fsqe = new SolrLibQueryException("ESL0011",
                            new Object[] { params.toString() });
                }
                fsqe.addException(e);
            }
            statusPolicy.sleep(QueryType.QUERY);
        }
        throw fsqe;
    }

    private QueryResponse queryInternal(final SolrParams params,
            final METHOD method) {

        String solrServerName = null;
        try {
            SolrServer solrServer = null;
            AtomicInteger minValue = null;
            // get a server which is  
            for (final Map.Entry<String, SolrServer> entry : solrServerMap
                    .entrySet()) {
                AtomicInteger count = accessCountMap.get(entry.getKey());
                if (count == null) {
                    // set count
                    count = new AtomicInteger(1);
                    final AtomicInteger accessCount = accessCountMap
                            .putIfAbsent(entry.getKey(), count);
                    if (accessCount != null) {
                        accessCount.getAndIncrement();
                        count = accessCount;
                    }
                }
                if ((minValue == null || count.get() < minValue.get())
                        && statusPolicy.isActive(QueryType.QUERY,
                                entry.getKey())) {
                    // active
                    minValue = count;
                    solrServerName = entry.getKey();
                    solrServer = entry.getValue();
                }
            }

            if (solrServer == null) {
                // If all server is unavailable, server group will be 
                // inactive at the next access
                throw new SolrLibException("ESL0002",
                        new Object[] { groupName });
            }

            // update count
            if (minValue.getAndIncrement() > MAX_LOAD_COUNT) {
                // clear all access counts
                accessCountMap.clear();
            }

            // status check
            if (!statusPolicy.isActive(QueryType.QUERY, solrServerName)) {
                throw new SolrLibServerNotAvailableException(groupName,
                        solrServerName);
            }

            final QueryResponse queryResponse = solrServer
                    .query(params, method);

            // clear error count
            errorCountMap.remove(solrServerName);

            return queryResponse;
        } catch (final SolrServerException e) {
            throw getQueryException(QueryType.QUERY, params, solrServerName, e);
        }
    }

    public NamedList<Object> request(final SolrRequest request) {
        // check this group status
        checkStatus(QueryType.REQUEST);

        SolrLibQueryException fsqe = null;
        final int maxRetryCount = statusPolicy
                .getMaxRetryCount(QueryType.REQUEST);
        for (int i = 0; i < maxRetryCount; i++) {
            try {
                return requestInternal(request);
            } catch (final Exception e) {
                if (fsqe == null) {
                    fsqe = new SolrLibQueryException("ESL0011",
                            new Object[] { request.toString() });
                }
                fsqe.addException(e);
            }
            statusPolicy.sleep(QueryType.REQUEST);
        }
        throw fsqe;
    }

    private NamedList<Object> requestInternal(final SolrRequest request) {

        String solrServerName = null;
        try {
            SolrServer solrServer = null;
            AtomicInteger minValue = null;
            // get a server which is  
            for (final Map.Entry<String, SolrServer> entry : solrServerMap
                    .entrySet()) {
                AtomicInteger count = accessCountMap.get(entry.getKey());
                if (count == null) {
                    // set count
                    count = new AtomicInteger(1);
                    final AtomicInteger accessCount = accessCountMap
                            .putIfAbsent(entry.getKey(), count);
                    if (accessCount != null) {
                        accessCount.getAndIncrement();
                        count = accessCount;
                    }
                }
                if ((minValue == null || count.get() < minValue.get())
                        && statusPolicy.isActive(QueryType.REQUEST,
                                entry.getKey())) {
                    // active
                    minValue = count;
                    solrServerName = entry.getKey();
                    solrServer = entry.getValue();
                }
            }

            if (solrServer == null) {
                // If all server is unavailable, server group will be 
                // inactive at the next access
                throw new SolrLibException("ESL0002",
                        new Object[] { groupName });
            }

            // update count
            if (minValue.getAndIncrement() > MAX_LOAD_COUNT) {
                // clear all access counts
                accessCountMap.clear();
            }

            // status check
            if (!statusPolicy.isActive(QueryType.REQUEST, solrServerName)) {
                throw new SolrLibServerNotAvailableException(groupName,
                        solrServerName);
            }

            final NamedList<Object> response = solrServer.request(request);

            // clear error count
            errorCountMap.remove(solrServerName);

            return response;
        } catch (final IOException e) {
            throw getQueryException(QueryType.REQUEST, request, solrServerName,
                    e);
        } catch (final SolrServerException e) {
            throw getQueryException(QueryType.REQUEST, request, solrServerName,
                    e);
        }
    }

    protected SolrLibException getQueryException(final QueryType queryType,
            final Object selectQuery, final String solrServerName,
            final Exception e) {
        if (solrServerName != null) {
            AtomicInteger errorCount = errorCountMap.get(solrServerName);
            if (errorCount == null) {
                // set a error count
                errorCount = new AtomicInteger(0);
                final AtomicInteger count = errorCountMap.putIfAbsent(
                        solrServerName, errorCount);
                if (count != null) {
                    errorCount = count;
                }
            }
            final int maxErrorCount = statusPolicy.getMaxErrorCount(queryType);
            if (errorCount.get() > maxErrorCount) {
                // deactivate a server
                statusPolicy.deactivate(queryType, solrServerName);
                // clear error count
                errorCountMap.remove(solrServerName);
                return new SolrLibException("ESL0003", new Object[] {
                        solrServerName, groupName, errorCount, maxErrorCount },
                        e);
            } else {
                // set a error count
                errorCount.getAndIncrement();
            }
            return new SolrLibException("ESL0004", new Object[] {
                    solrServerName, groupName, errorCount, maxErrorCount }, e);
        } else {
            return new SolrLibException("ESL0005",
                    new Object[] { selectQuery.toString() }, e);
        }
    }

    protected <T> Collection<T> updateQueryCallback(final QueryType queryType,
            final UpdateProcessCallback<T> upc) {
        final List<T> resultList = new ArrayList<T>();
        synchronized (statusPolicy) {
            for (final Map.Entry<String, SolrServer> entry : solrServerMap
                    .entrySet()) {
                final String serverName = entry.getKey();
                if (statusPolicy.isActive(queryType, serverName)) {
                    executeUpdateQuery(queryType, upc, resultList, entry,
                            serverName, 0);
                }
            }

        }
        return resultList;
    }

    protected <T> void executeUpdateQuery(final QueryType queryType,
            final UpdateProcessCallback<T> upc, final List<T> resultList,
            final Map.Entry<String, SolrServer> entry, final String serverName,
            final int retryCount) {
        try {
            resultList.add(upc.callback(entry.getValue()));
        } catch (final Exception e) {
            logger.log(
                    Logger.format("WSL0003", new Object[] { serverName, upc,
                            retryCount }), e);

            if (retryCount > statusPolicy.getMaxRetryCount(queryType)) {
                // set to corrupted
                statusPolicy.deactivate(queryType, serverName);
            } else {
                statusPolicy.sleep(queryType);

                // retry
                executeUpdateQuery(queryType, upc, resultList, entry,
                        serverName, retryCount + 1);
            }
        }
    }

    public boolean isActive(final QueryType queryType) {
        return statusPolicy.isActive(queryType, solrServerMap.keySet());
    }

    protected void checkStatus(final QueryType queryType) {
        // if this server group is unavailable, throws SolrLibException.
        if (!statusPolicy.isActive(queryType, solrServerMap.keySet())) {
            throw new SolrLibGroupNotAvailableException(groupName);
        }
    }

    public void updateStatus() {
        // check a status for all servers 
        synchronized (statusPolicy) {
            for (final Map.Entry<String, SolrServer> entry : solrServerMap
                    .entrySet()) {
                final String serverName = entry.getKey();
                if (!statusPolicy.isActive(QueryType.PING, serverName)) {
                    // you can activate a server when the status is inactive only..
                    try {
                        final SolrPingResponse pingResponse = entry.getValue()
                                .ping();
                        if (pingResponse.getStatus() == 0) {
                            // activate a server
                            statusPolicy.activate(QueryType.PING, serverName);
                        } else {
                            logger.log("WSL0002", new Object[] { serverName,
                                    pingResponse.getStatus() });
                            // inactivate a server
                            statusPolicy.deactivate(QueryType.PING, serverName);
                        }
                    } catch (final Exception e) {
                        logger.log(Logger.format("WSL0001",
                                new Object[] { serverName }), e);
                        statusPolicy.deactivate(QueryType.PING, serverName);
                    }
                }
            }
        }
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(final String groupName) {
        this.groupName = groupName;
    }

    public StatusPolicy getStatusPolicy() {
        return statusPolicy;
    }

    public void setStatusPolicy(final StatusPolicy statusPolicy) {
        this.statusPolicy = statusPolicy;
    }

    protected interface UpdateProcessCallback<V> {
        V callback(SolrServer solrServer);
    }

}
