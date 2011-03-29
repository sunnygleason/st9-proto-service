package itest.com.g414.st9.proto.service.store;

import org.testng.annotations.Test;

import utest.com.g414.st9.proto.service.store.CounterServiceTestBase;

import com.g414.st9.proto.service.store.MySQLKeyValueStorage;
import com.google.inject.Module;

@Test
public class MySQLCounterServiceTest extends CounterServiceTestBase {
    @Override
    public Module getStorageModule() {
        return new MySQLKeyValueStorage.MySQLKeyValueStorageModule();
    }
}