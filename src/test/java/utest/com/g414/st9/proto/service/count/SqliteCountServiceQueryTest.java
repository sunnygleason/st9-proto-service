package utest.com.g414.st9.proto.service.count;

import org.testng.annotations.Test;

import com.g414.st9.proto.service.store.SqliteKeyValueStorage.SqliteKeyValueStorageModule;
import com.google.inject.Module;

@Test
public class SqliteCountServiceQueryTest extends CountServiceQueryTestBase {
    public Module getKeyValueStorageModule() {
        return new SqliteKeyValueStorageModule();
    }
}