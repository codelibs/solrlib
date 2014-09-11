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
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.codelibs.core.msg.MessageFormatter;
import org.codelibs.solr.lib.exception.SolrLibException;
import org.codelibs.solr.lib.exception.SolrLibGroupNotAvailableException;
import org.codelibs.solr.lib.exception.SolrLibQueryException;
import org.codelibs.solr.lib.exception.SolrLibServerNotAvailableException;
import org.codelibs.solr.lib.exception.SolrLibServiceException;
import org.codelibs.solr.lib.policy.QueryType;
import org.codelibs.solr.lib.policy.StatusPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SolrGroup is a proxy implemetation to a group of SolrServer instances.
 *
 * @author shinsuke
 *
 */
public class SolrGroup {

    private static final Logger logger = LoggerFactory
            .getLogger(SolrGroup.class);

    protected static final int MAX_LOAD_COUNT = Integer.MAX_VALUE / 2;

    protected Map<String, SolrServer> solrServerMap = new LinkedHashMap<String, SolrServer>();

    protected String groupName;

    protected ConcurrentHashMap<String, AtomicInteger> accessCountMap = new ConcurrentHashMap<String, AtomicInteger>();

    protected ConcurrentHashMap<String, AtomicInteger> errorCountMap = new ConcurrentHashMap<String, AtomicInteger>();

    protected StatusPolicy statusPolicy;

    public void addServer(final String name, final SolrServer solrServer) {
        solrServerMap.put(name, solrServer);
    }

    public String[] getServerNames() {
        return solrServerMap.keySet().toArray(new String[solrServerMap.size()]);
    }

    /**
     * Adds a collection of documents
     * @param docs  the collection of documents
     */
    public Collection<UpdateResponse> add(
            final Collection<SolrInputDocument> docs) {
        return add(docs, -1);
    }

    /**
     * Adds a collection of documents, specifying max time before they become committed
     * @param docs  the collection of documents
     * @param commitWithinMs  max time (in ms) before a commit will happen
     */
    public Collection<UpdateResponse> add(
            final Collection<SolrInputDocument> docs, final int commitWithinMs) {
        // check this group status
        checkStatus(QueryType.ADD);

        return updateQueryCallback(QueryType.ADD,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.add(docs, commitWithinMs);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    /**
     * Adds a single document
     * @param doc  the input document
     */
    public Collection<UpdateResponse> add(final SolrInputDocument doc) {
        return add(doc, -1);
    }

    /**
     * Adds a single document specifying max time before it becomes committed
     * @param doc  the input document
     * @param commitWithinMs  max time (in ms) before a commit will happen
     */
    public Collection<UpdateResponse> add(final SolrInputDocument doc,
            final int commitWithinMs) {
        // check this group status
        checkStatus(QueryType.ADD);

        return updateQueryCallback(QueryType.ADD,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.add(doc, commitWithinMs);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    /**
     * Adds a single bean
     * @param obj  the input bean
     */
    public Collection<UpdateResponse> addBean(final Object obj) {
        return addBean(obj, -1);
    }

    /**
     * Adds a single bean specifying max time before it becomes committed
     * @param obj  the input bean
     * @param commitWithinMs  max time (in ms) before a commit will happen
     */
    public Collection<UpdateResponse> addBean(final Object obj,
            final int commitWithinMs) {
        // check this group status
        checkStatus(QueryType.ADD);

        return updateQueryCallback(QueryType.ADD,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.addBean(obj, commitWithinMs);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    /**
     * Adds a collection of beans
     * @param beans  the collection of beans
     */
    public Collection<UpdateResponse> addBean(final Collection<?> beans) {
        return addBean(beans, -1);
    }

    /**
     * Adds a collection of beans specifying max time before they become committed
     * @param beans  the collection of beans
     * @param commitWithinMs  max time (in ms) before a commit will happen
     */
    public Collection<UpdateResponse> addBean(final Collection<?> beans,
            final int commitWithinMs) {
        // check this group status
        checkStatus(QueryType.ADD);

        return updateQueryCallback(QueryType.ADD,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.addBeans(beans, commitWithinMs);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    /**
     * Performs an explicit commit, causing pending documents to be committed for indexing
     * <p>
     * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
     */
    public Collection<UpdateResponse> commit() {
        return commit(true, true, false);
    }

    /**
     * Performs an explicit commit, causing pending documents to be committed for indexing
     * @param waitFlush  block until index changes are flushed to disk
     * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible
     */
    public Collection<UpdateResponse> commit(final boolean waitFlush,
            final boolean waitSearcher) {
        return commit(waitFlush, waitSearcher, false);
    }

    /**
     * Performs an explicit commit, causing pending documents to be committed for indexing
     * @param waitFlush  block until index changes are flushed to disk
     * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible
     * @param softCommit makes index changes visible while neither fsync-ing index files nor writing a new index descriptor
     */
    public Collection<UpdateResponse> commit(final boolean waitFlush,
            final boolean waitSearcher, final boolean softCommit) {
        return commit(waitFlush, waitSearcher, softCommit, false);
    }

    /**
     * Performs an explicit commit, causing pending documents to be committed for indexing
     * @param waitFlush  block until index changes are flushed to disk
     * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible
     * @param softCommit makes index changes visible while neither fsync-ing index files nor writing a new index descriptor
     * @param expungeDeletes merge segments with deletes away
     */
    public Collection<UpdateResponse> commit(final boolean waitFlush,
            final boolean waitSearcher, final boolean softCommit,
            final boolean expungeDeletes) {
        // check this group status
        checkStatus(QueryType.COMMIT);

        return updateQueryCallback(QueryType.COMMIT,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return new UpdateRequest()
                                    .setAction(UpdateRequest.ACTION.COMMIT,
                                            waitFlush, waitSearcher, 1,
                                            softCommit, expungeDeletes)
                                    .process(solrServer);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    /**
     * Performs a rollback of all non-committed documents pending.
     * <p>
     * Note that this is not a true rollback as in databases. Content you have previously
     * added may have been committed due to autoCommit, buffer full, other client performing
     * a commit etc.
     */
    public Collection<UpdateResponse> rollback() {
        // check this group status
        checkStatus(QueryType.ROLLBACK);

        return updateQueryCallback(QueryType.ROLLBACK,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.rollback();
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    /**
     * Deletes documents from the index based on a query
     * @param query  the query expressing what documents to delete
     */
    public Collection<UpdateResponse> deleteByQuery(final String query) {
        return deleteByQuery(query, -1);
    }

    /**
     * Deletes documents from the index based on a query, specifying max time before commit
     * @param query  the query expressing what documents to delete
     * @param commitWithinMs  max time (in ms) before a commit will happen
     */
    public Collection<UpdateResponse> deleteByQuery(final String query,
            final int commitWithinMs) {
        // check this group status
        checkStatus(QueryType.DELETE);

        return updateQueryCallback(QueryType.DELETE,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.deleteByQuery(query,
                                    commitWithinMs);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    /**
     * Deletes a single document by unique ID
     * @param id  the ID of the document to delete
     */
    public Collection<UpdateResponse> deleteById(final String id) {
        return deleteById(id, -1);
    }

    /**
     * Deletes a single document by unique ID, specifying max time before commit
     * @param id  the ID of the document to delete
     * @param commitWithinMs  max time (in ms) before a commit will happen
     */
    public Collection<UpdateResponse> deleteById(final String id,
            final int commitWithinMs) {
        // check this group status
        checkStatus(QueryType.DELETE);

        return updateQueryCallback(QueryType.DELETE,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.deleteById(id, commitWithinMs);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    /**
     * Deletes a list of documents by unique ID
     * @param ids  the list of document IDs to delete
     */
    public Collection<UpdateResponse> deleteById(final List<String> ids) {
        return deleteById(ids, -1);
    }

    /**
     * Deletes a list of documents by unique ID, specifying max time before commit
     * @param ids  the list of document IDs to delete
     * @param commitWithinMs  max time (in ms) before a commit will happen
     */
    public Collection<UpdateResponse> deleteById(final List<String> ids,
            final int commitWithinMs) {
        // check this group status
        checkStatus(QueryType.DELETE);

        return updateQueryCallback(QueryType.DELETE,
                new UpdateProcessCallback<UpdateResponse>() {
                    @Override
                    public UpdateResponse callback(final SolrServer solrServer) {
                        try {
                            return solrServer.deleteById(ids, commitWithinMs);
                        } catch (final Exception e) {
                            throw new SolrLibException(e);
                        }
                    }
                });
    }

    /**
     * Performs an explicit optimize, causing a merge of all segments to one.
     * <p>
     * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
     * <p>
     * Note: In most cases it is not required to do explicit optimize
     */
    public Collection<UpdateResponse> optimize() {
        return optimize(true, true, 1);
    }

    /**
     * Performs an explicit optimize, causing a merge of all segments to one.
     * <p>
     * Note: In most cases it is not required to do explicit optimize
     * @param waitFlush  block until index changes are flushed to disk
     * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible
     */
    public Collection<UpdateResponse> optimize(final boolean waitFlush,
            final boolean waitSearcher) {
        return optimize(waitFlush, waitSearcher, 1);

    }

    /**
     * Performs an explicit optimize, causing a merge of all segments to one.
     * <p>
     * Note: In most cases it is not required to do explicit optimize
     * @param waitFlush  block until index changes are flushed to disk
     * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible
     * @param maxSegments  optimizes down to at most this number of segments
     */
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

    /**
     * Issues a ping request to check if the server is alive
     */
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

    /**
     * Query solr, and stream the results.  Unlike the standard query, this will
     * send events for each Document rather then add them to the QueryResponse.
     *
     * Although this function returns a 'QueryResponse' it should be used with care
     * since it excludes anything that was passed to callback.  Also note that
     * future version may pass even more info to the callback and may not return
     * the results in the QueryResponse.
     *
     */
    public QueryResponse queryAndStreamResponse(final SolrParams params,
            final StreamingResponseCallback callback) {
        // check this group status
        checkStatus(QueryType.QUERY);

        SolrLibServiceException fsqe = null;
        final int maxRetryCount = statusPolicy
                .getMaxRetryCount(QueryType.QUERY);
        for (int i = 0; i < maxRetryCount; i++) {
            try {
                return queryInternal(QueryType.QUERY, params,
                        new UpdateProcessCallback<QueryResponse>() {
                            @Override
                            public QueryResponse callback(
                                    final SolrServer solrServer)
                                    throws SolrServerException, IOException {
                                return solrServer.queryAndStreamResponse(
                                        params, callback);
                            }
                        });
            } catch (final SolrLibQueryException
                    | SolrLibServerNotAvailableException e) {
                throw e;
            } catch (final Exception e) {
                if (fsqe == null) {
                    fsqe = new SolrLibServiceException("ESL0011",
                            new Object[] { params.toString() });
                }
                fsqe.addException(e);
            }
            statusPolicy.sleep(QueryType.QUERY);
        }

        throw fsqe;
    }

    /**
     * Performs a query to the Solr server
     * @param params  an object holding all key/value parameters to send along the request
     */
    public QueryResponse query(final SolrParams params) {
        return query(params, SolrRequest.METHOD.GET);
    }

    /**
     * Performs a query to the Solr server
     * @param params  an object holding all key/value parameters to send along the request
     * @param method  specifies the HTTP method to use for the request, such as GET or POST
     */
    public QueryResponse query(final SolrParams params, final METHOD method) {
        // check this group status
        checkStatus(QueryType.QUERY);

        SolrLibServiceException fsqe = null;
        final int maxRetryCount = statusPolicy
                .getMaxRetryCount(QueryType.QUERY);
        for (int i = 0; i < maxRetryCount; i++) {
            try {
                return queryInternal(QueryType.QUERY, params,
                        new UpdateProcessCallback<QueryResponse>() {
                            @Override
                            public QueryResponse callback(
                                    final SolrServer solrServer)
                                    throws SolrServerException, IOException {
                                return solrServer.query(params, method);
                            }
                        });
            } catch (final SolrLibQueryException
                    | SolrLibServerNotAvailableException e) {
                throw e;
            } catch (final Exception e) {
                if (fsqe == null) {
                    fsqe = new SolrLibServiceException("ESL0011",
                            new Object[] { params.toString() });
                }
                fsqe.addException(e);
            }
            statusPolicy.sleep(QueryType.QUERY);
        }

        throw fsqe;
    }

    /**
     * SolrServer implementations need to implement how a request is actually processed
     */
    public NamedList<Object> request(final SolrRequest request) {
        // check this group status
        checkStatus(QueryType.REQUEST);

        SolrLibServiceException fsqe = null;
        final int maxRetryCount = statusPolicy
                .getMaxRetryCount(QueryType.REQUEST);
        for (int i = 0; i < maxRetryCount; i++) {
            try {
                return queryInternal(QueryType.REQUEST, request,
                        new UpdateProcessCallback<NamedList<Object>>() {
                            @Override
                            public NamedList<Object> callback(
                                    final SolrServer solrServer)
                                    throws SolrServerException, IOException {
                                return solrServer.request(request);
                            }
                        });
            } catch (final SolrLibQueryException
                    | SolrLibServerNotAvailableException e) {
                throw e;
            } catch (final Exception e) {
                if (fsqe == null) {
                    fsqe = new SolrLibServiceException("ESL0011",
                            new Object[] { request.toString() });
                }
                fsqe.addException(e);
            }
            statusPolicy.sleep(QueryType.REQUEST);
        }
        throw fsqe;
    }

    protected <RESULT> RESULT queryInternal(final QueryType queryType,
            final Object selectQuery,
            final UpdateProcessCallback<RESULT> callback) {

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
                        && statusPolicy.isActive(queryType, entry.getKey())) {
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
            if (!statusPolicy.isActive(queryType, solrServerName)) {
                throw new SolrLibServerNotAvailableException(groupName,
                        solrServerName);
            }

            final RESULT result = callback.callback(solrServer);

            // clear error count
            errorCountMap.remove(solrServerName);

            return result;
        } catch (final Exception e) {
            throw getQueryException(queryType, selectQuery, solrServerName, e);
        }
    }

    protected SolrLibException getQueryException(final QueryType queryType,
            final Object selectQuery, final String solrServerName,
            final Exception e) {
        if (e instanceof SolrException) {
            switch (((SolrException) e).code()) {
            case 404: // NOT_FOUND
                throw new SolrLibServerNotAvailableException(groupName,
                        solrServerName);
            case 400: // BAD_REQUEST
            case 500: // SERVER_ERROR
                // an invalid query
                throw new SolrLibQueryException("ESL0013",
                        new Object[] { selectQuery }, e);
            default:
                break;
            }
        }

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
            logger.warn(
                    MessageFormatter.getSimpleMessage("WSL0003", new Object[] {
                            serverName, upc, retryCount }), e);

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
                            logger.warn(MessageFormatter.getSimpleMessage(
                                    "WSL0002", new Object[] { serverName,
                                            pingResponse.getStatus() }));
                            // inactivate a server
                            statusPolicy.deactivate(QueryType.PING, serverName);
                        }
                    } catch (final Exception e) {
                        logger.warn(MessageFormatter.getSimpleMessage(
                                "WSL0001", new Object[] { serverName }), e);
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
        V callback(SolrServer solrServer) throws SolrServerException,
                IOException;
    }

}
