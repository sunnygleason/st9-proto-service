package com.g414.st9.proto.service;

import org.testng.annotations.Test;

import com.g414.st9.proto.service.EmptyServlet;

public class EmptyServletTest {
    /**
     * EmptyServlet DoGet should throw up violently.
     */
    @Test(expectedExceptions = IllegalStateException.class)
    public void testEmptyServlet() throws Exception {
        (new EmptyServlet()).doGet(null, null);
    }
}
