package com.badoo.server;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Created by krash on 05.03.18.
 */
@Data
public class JsonRpcResponse<T> {

    private final String jsonrpc = "2.0";
    private final T result;

    @Setter
    @Accessors(fluent = true)
    private JsonRpcError error;
}
