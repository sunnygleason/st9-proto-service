package utest.com.g414.st9.proto.service.store;

import org.testng.annotations.Test;

import com.g414.st9.proto.service.store.H2KeyValueStorage.H2KeyValueStorageModule;
import com.google.inject.Module;

@Test
public class H2CounterServiceTest extends CounterServiceTestBase {
    @Override
    public Module getStorageModule() {
        return new H2KeyValueStorageModule();
    }
}