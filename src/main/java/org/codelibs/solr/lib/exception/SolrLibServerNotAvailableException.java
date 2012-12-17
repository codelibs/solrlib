package org.codelibs.solr.lib.exception;

/**
 * Group is not available.
 * 
 * @author shinsuke
 *
 */
public class SolrLibServerNotAvailableException extends SolrLibException {

    private static final long serialVersionUID = 1L;

    public SolrLibServerNotAvailableException(final String groupName,
            final String serverName) {
        super("ESL0012", new Object[] { groupName, serverName });
    }

}
