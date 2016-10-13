package com.exasol.utils;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;

public class UdfUtils {
    
    public static void tryAttachToOutputService(String ip, int port) {
        // Start before: udf_debug.py
        try {
            @SuppressWarnings("resource")
            Socket socket = new Socket(ip, port);
            PrintStream out = new PrintStream(socket.getOutputStream(), true);
            System.setOut(out);
            System.out.println("\n\n\nAttached to outputservice");
        } catch (Exception ex) {} // could not start output server}
    }

    public static String traceToString(Exception ex) {
        StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }
}
