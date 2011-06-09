package itest.com.g414.st9.proto.service.count;

import org.testng.annotations.Test;

import utest.com.g414.st9.proto.service.count.CountServiceQueryTestBase;

import com.g414.st9.proto.service.store.MySQLKeyValueStorage.MySQLKeyValueStorageModule;
import com.google.inject.Module;

@Test
public class MySQLCountServiceQueryTest extends CountServiceQueryTestBase {
    public Module getKeyValueStorageModule() {
        return new MySQLKeyValueStorageModule();
    }
}