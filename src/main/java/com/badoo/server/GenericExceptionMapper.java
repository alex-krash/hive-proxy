package com.badoo.server;

import lombok.Value;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Created by krash on 05.03.18.
 */
@Provider
public class GenericExceptionMapper implements ExceptionMapper<Exception> {

    public static String throwableToString(Throwable e) {
        if (e == null)
            return null;
        StringWriter errorStackTrace = new StringWriter();
        e.printStackTrace(new PrintWriter(errorStackTrace));
        return errorStackTrace.toString();
    }

    @Override
    public Response toResponse(Exception ex) {

        int status = 500;
        if (ex instanceof WebApplicationException) {
            status = ((WebApplicationException) ex).getResponse().getStatus();
        }
        String message = ex.getMessage();
        if (null == message && null != ex.getCause()) {
            message = ex.getCause().getMessage();
        }

        JsonRpcResponse<Object> resp = new JsonRpcResponse<>(null).error(new JsonRpcError(status, message, new Trace(ex.getStackTrace())));

        return Response.status(status)
                .entity(resp)
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

    @Value
    private static class Trace {
        private final Object trace;
    }
}
