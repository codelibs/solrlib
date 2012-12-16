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

package org.codelibs.solr.lib.policy;

import java.util.Set;

public interface StatusPolicy {

    void activate(QueryType queryType, String serverName);

    void deactivate(QueryType queryType, String serverName);

    boolean isActive(QueryType queryType, Set<String> serverNameSet);

    boolean isActive(QueryType queryType, String serverName);

    void sleep(QueryType queryType);

    int getMaxRetryCount(QueryType queryType);

    int getMaxErrorCount(QueryType queryType);

}