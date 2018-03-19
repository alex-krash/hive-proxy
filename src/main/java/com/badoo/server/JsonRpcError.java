package com.badoo.server;

import lombok.Data;

/**
 * Created by krash on 05.03.18.
 */
@Data
public class JsonRpcError {
    private final int code;
    private final String message;
    private final Object data;
}
