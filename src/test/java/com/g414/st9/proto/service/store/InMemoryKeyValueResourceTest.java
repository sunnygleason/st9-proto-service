package com.g414.st9.proto.service.store;

import org.testng.annotations.Test;

import com.g414.st9.proto.service.store.InMemoryKeyValueStorage.InMemoryKeyValueStorageModule;
import com.google.inject.Module;

@Test
public class InMemoryKeyValueResourceTest extends KeyValueResourceTestBase {
    public Module getKeyValueStorageModule() {
        return new InMemoryKeyValueStorageModule();
    }
}