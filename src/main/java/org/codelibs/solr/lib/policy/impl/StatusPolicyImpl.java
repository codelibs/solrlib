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

import org.codelibs.core.util.DynamicProperties;
import org.codelibs.solr.lib.policy.QueryType;
import org.codelibs.solr.lib.policy.StatusPolicy;

public class StatusPolicyImpl implements StatusPolicy {

    protected static final String STATUS_PREFIX = "status.";

    protected static final String INDEX_PREFIX = "index.";

    protected static final String ACTIVE = "active";

    protected static final String INACTIVE = "inactive";

    protected static final String COMPLETED = "completed";

    protected static final String UNFINISHED = "unfinished";

    protected DynamicProperties solrServerProperties;

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
        case ADD:
        case COMMIT:
        case DELETE:
        case OPTIMIZE:
        case PING:
        case QUERY:
        case REQUEST:
        default:
            solrServerProperties.setProperty(getStatusKey(serverName), ACTIVE);
            solrServerProperties.store();
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
            solrServerProperties.setProperty(getIndexKey(serverName),
                    UNFINISHED);
        case PING:
        case QUERY:
        case REQUEST:
            solrServerProperties
                    .setProperty(getStatusKey(serverName), INACTIVE);
            solrServerProperties.store();
        default:
            break;
        }
    }

    /* (non-Javadoc)
     * @see org.codelibs.solr.lib.policy.StatusPolicy#isAvailable(org.codelibs.solr.lib.policy.QueryType, java.util.Set)
     */
    @Override
    public boolean isAvailable(final QueryType queryType,
            final Set<String> serverNameSet) {

        // check the number of an active server
        int numOfActive = 0;
        for (final String serverName : serverNameSet) {
            if (isAvailable(queryType, serverName)) {
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
    public boolean isAvailable(final QueryType queryType,
            final String serverName) {
        final String serverStatus = solrServerProperties.getProperty(
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
            if (ACTIVE.equals(serverStatus)) {
                final String serverIndex = solrServerProperties.getProperty(
                        getIndexKey(serverName), COMPLETED);
                return COMPLETED.equals(serverIndex);
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
}
