package com.exasol.auth.kerberos;

/**
 * Exception for errors occurring during creation of the Kerberos configuration or its files.
 */
public class KerberosConfigurationCreatorException extends RuntimeException {
    private static final long serialVersionUID = -7910268166902081246L;

    /**
     * Create a new instance of a {@link KerberosConfigurationCreatorException}.
     *
     * @param message error message
     */
    public KerberosConfigurationCreatorException(final String message) {
        super(message);
    }

    /**
     * Create a new instance of a {@link KerberosConfigurationCreatorException}.
     *
     * @param message error message
     * @param cause   cause
     */
    public KerberosConfigurationCreatorException(final String message, final Throwable cause) {
        super(message, cause);
    }
}