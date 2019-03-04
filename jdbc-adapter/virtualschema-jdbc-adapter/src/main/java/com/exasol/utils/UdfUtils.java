package com.exasol.utils;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;

public class UdfUtils {
    public static OutputStream tryAttachToOutputService(final String ip, final int port) {
        try (final Socket socket = new Socket(ip, port)) {
            return socket.getOutputStream();
        } catch (final Exception ex) {
            return null;
        }
    }

    public static String traceToString(final Exception ex) {
        final StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }
}
