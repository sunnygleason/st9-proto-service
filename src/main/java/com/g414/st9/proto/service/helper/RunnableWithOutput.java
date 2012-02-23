package com.g414.st9.proto.service.helper;

import java.io.OutputStream;

public interface RunnableWithOutput {
    public void run(OutputStream output) throws Exception;
}
