package itest.com.g414.st9.proto.service.store;

import org.testng.annotations.Test;

import utest.com.g414.st9.proto.service.store.KeyValueResourceTestBase;

import com.g414.st9.proto.service.store.MySQLKeyValueStorage.MySQLKeyValueStorageModule;
import com.google.inject.Module;

@Test
public class MySQLKeyValueResourceTest extends KeyValueResourceTestBase {
    public Module getKeyValueStorageModule() {
        return new MySQLKeyValueStorageModule();
    }
}