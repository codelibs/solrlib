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

package org.codelibs.solr.lib.policy.impl;

import java.util.Set;

import org.codelibs.core.misc.DynamicProperties;
import org.codelibs.solr.lib.policy.QueryType;
import org.codelibs.solr.lib.policy.StatusPolicy;

public class StatusPolicyImpl implements StatusPolicy {

    public static final String STATUS_PREFIX = "status.";

    public static final String INDEX_PREFIX = "index.";

    public static final String ACTIVE = "active";

    public static final String INACTIVE = "inactive";

    public static final String COMPLETED = "completed";

    public static final String UNFINISHED = "unfinished";

    public static final String READY = "ready";

    protected DynamicProperties solrGroupProperties;

    /** the number of minimum active servers */
    protected int minSelectServer = 1;

    protected int minUpdateServer = 1;

    protected long retrySelectQueryInterval = 500;

    protected long retryUpdateQueryInterval = 500;

    /** a max error count */
    protected int maxErrorCount = 3;

    /** a max retry count */
    protected int maxRetryUpdateQueryCount = 3;

    /** a max retry count */
    protected int maxRetrySelectQueryCount = 3;

    /* (non-Javadoc)
     * @see org.codelibs.solr.lib.policy.StatusPolicy#activate(org.codelibs.solr.lib.policy.QueryType, java.lang.String)
     */
    @Override
    public void activate(final QueryType queryType, final String serverName) {
        switch (queryType) {
        case COMMIT:
        case OPTIMIZE:
        case ROLLBACK:
            solrGroupProperties.setProperty(getIndexKey(serverName), COMPLETED);
            solrGroupProperties.setProperty(getStatusKey(serverName), ACTIVE);
            solrGroupProperties.store();
            break;
        case ADD:
        case DELETE:
            solrGroupProperties.setProperty(getIndexKey(serverName), READY);
            solrGroupProperties.setProperty(getStatusKey(serverName), ACTIVE);
            solrGroupProperties.store();
            break;
        case PING:
        case QUERY:
        case REQUEST:
        default:
            solrGroupProperties.setProperty(getStatusKey(serverName), ACTIVE);
            solrGroupProperties.store();
            break;
        }
    }

    /* (non-Javadoc)
     * @see org.codelibs.solr.lib.policy.StatusPolicy#deactivate(org.codelibs.solr.lib.policy.QueryType, java.lang.String)
     */
    @Override
    public void deactivate(final QueryType queryType, final String serverName) {
        switch (queryType) {
        case ADD:
        case COMMIT:
        case DELETE:
        case OPTIMIZE:
        case ROLLBACK:
            solrGroupProperties
                    .setProperty(getIndexKey(serverName), UNFINISHED);
            solrGroupProperties.setProperty(getStatusKey(serverName), INACTIVE);
            solrGroupProperties.store();
            break;
        case PING:
        case QUERY:
        case REQUEST:
            solrGroupProperties.setProperty(getStatusKey(serverName), INACTIVE);
            solrGroupProperties.store();
            break;
        default:
            break;
        }
    }

    /* (non-Javadoc)
     * @see org.codelibs.solr.lib.policy.StatusPolicy#isAvailable(org.codelibs.solr.lib.policy.QueryType, java.util.Set)
     */
    @Override
    public boolean isActive(final QueryType queryType,
            final Set<String> serverNameSet) {

        // check the number of an active server
        int numOfActive = 0;
        for (final String serverName : serverNameSet) {
            if (isActive(queryType, serverName)) {
                // active
                numOfActive++;
            }
        }

        switch (queryType) {
        case PING:
        case QUERY:
        case REQUEST:
            if (numOfActive >= minSelectServer) {
                // this server group is active
                return true;
            }
            break;
        case ADD:
        case COMMIT:
        case DELETE:
        case OPTIMIZE:
        case ROLLBACK:
            if (numOfActive >= minUpdateServer) {
                // this server group is active
                return true;
            }
            break;
        default:
            break;
        }

        return false;
    }

    /* (non-Javadoc)
     * @see org.codelibs.solr.lib.policy.StatusPolicy#isAvailable(org.codelibs.solr.lib.policy.QueryType, java.lang.String)
     */
    @Override
    public boolean isActive(final QueryType queryType, final String serverName) {
        final String serverStatus = solrGroupProperties.getProperty(
                getStatusKey(serverName), ACTIVE);
        switch (queryType) {
        case PING:
        case QUERY:
        case REQUEST:
            if (ACTIVE.equals(serverStatus)) {
                return true;
            }
            // TODO time check
            break;
        case ADD:
        case COMMIT:
        case DELETE:
        case OPTIMIZE:
        case ROLLBACK:
            if (ACTIVE.equals(serverStatus)) {
                final String serverIndex = solrGroupProperties.getProperty(
                        getIndexKey(serverName), COMPLETED);
                return COMPLETED.equals(serverIndex)
                        || READY.equals(serverIndex);
            }
            break;
        default:
            break;
        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.codelibs.solr.lib.policy.StatusPolicy#sleep(org.codelibs.solr.lib.policy.QueryType)
     */
    @Override
    public void sleep(final QueryType queryType) {
        try {
            switch (queryType) {
            case PING:
            case QUERY:
            case REQUEST:
                Thread.sleep(retrySelectQueryInterval);
                break;
            case ADD:
            case COMMIT:
            case DELETE:
            case OPTIMIZE:
            case ROLLBACK:
                Thread.sleep(retryUpdateQueryInterval);
                break;
            default:
                break;
            }
        } catch (final InterruptedException e) {
            // ignore
        }
    }

    @Override
    public int getMaxRetryCount(final QueryType queryType) {
        switch (queryType) {
        case PING:
        case QUERY:
        case REQUEST:
            return maxRetrySelectQueryCount;
        case ADD:
        case COMMIT:
        case DELETE:
        case OPTIMIZE:
        case ROLLBACK:
            return maxRetryUpdateQueryCount;
        default:
            break;
        }
        return 0;
    }

    @Override
    public int getMaxErrorCount(final QueryType queryType) {
        return maxErrorCount;
    }

    protected String getStatusKey(final String serverName) {
        return STATUS_PREFIX + serverName;
    }

    protected String getIndexKey(final String serverName) {
        return INDEX_PREFIX + serverName;
    }

    public DynamicProperties getSolrGroupProperties() {
        return solrGroupProperties;
    }

    public void setSolrGroupProperties(
            final DynamicProperties solrGroupProperties) {
        this.solrGroupProperties = solrGroupProperties;
    }

    public int getMinSelectServer() {
        return minSelectServer;
    }

    public void setMinSelectServer(final int minSelectServer) {
        this.minSelectServer = minSelectServer;
    }

    public int getMinUpdateServer() {
        return minUpdateServer;
    }

    public void setMinUpdateServer(final int minUpdateServer) {
        this.minUpdateServer = minUpdateServer;
    }

    public long getRetrySelectQueryInterval() {
        return retrySelectQueryInterval;
    }

    public void setRetrySelectQueryInterval(final long retrySelectQueryInterval) {
        this.retrySelectQueryInterval = retrySelectQueryInterval;
    }

    public long getRetryUpdateQueryInterval() {
        return retryUpdateQueryInterval;
    }

    public void setRetryUpdateQueryInterval(final long retryUpdateQueryInterval) {
        this.retryUpdateQueryInterval = retryUpdateQueryInterval;
    }

    public int getMaxErrorCount() {
        return maxErrorCount;
    }

    public void setMaxErrorCount(final int maxErrorCount) {
        this.maxErrorCount = maxErrorCount;
    }

    public int getMaxRetryUpdateQueryCount() {
        return maxRetryUpdateQueryCount;
    }

    public void setMaxRetryUpdateQueryCount(final int maxRetryUpdateQueryCount) {
        this.maxRetryUpdateQueryCount = maxRetryUpdateQueryCount;
    }

    public int getMaxRetrySelectQueryCount() {
        return maxRetrySelectQueryCount;
    }

    public void setMaxRetrySelectQueryCount(final int maxRetrySelectQueryCount) {
        this.maxRetrySelectQueryCount = maxRetrySelectQueryCount;
    }
}
