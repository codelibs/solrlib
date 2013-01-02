package org.codelibs.solr.lib.exception;

public class SolrLibQueryException extends SolrLibException {

    private static final long serialVersionUID = 1L;

    public SolrLibQueryException(final String messageCode, final Object[] args) {
        super(messageCode, args);
    }

    public SolrLibQueryException(final String messageCode, final Object[] args,
            final Throwable cause) {
        super(messageCode, args, cause);
    }

}
