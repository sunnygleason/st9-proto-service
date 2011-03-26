package itest.com.g414.st9.proto.service.index;

import org.testng.annotations.Test;

import utest.com.g414.st9.proto.service.index.SecondaryIndexQueryTestBase;

import com.g414.st9.proto.service.store.MySQLKeyValueStorage;
import com.google.inject.Module;

@Test
public class MySQLSecondaryIndexQueryTest extends SecondaryIndexQueryTestBase {
    public Module getKeyValueStorageModule() {
        return new MySQLKeyValueStorage.MySQLKeyValueStorageModule();
    }
}