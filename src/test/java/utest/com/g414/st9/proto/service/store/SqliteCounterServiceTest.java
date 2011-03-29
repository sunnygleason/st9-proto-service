package utest.com.g414.st9.proto.service.store;

import org.testng.annotations.Test;

import com.g414.st9.proto.service.store.SqliteKeyValueStorage.SqliteKeyValueStorageModule;
import com.google.inject.Module;

@Test
public class SqliteCounterServiceTest extends CounterServiceTestBase {
    @Override
    public Module getStorageModule() {
        return new SqliteKeyValueStorageModule();
    }
}