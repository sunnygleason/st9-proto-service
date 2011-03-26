package utest.com.g414.st9.proto.service.store;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.store.KeyEncryptionHelper;
import com.g414.st9.proto.service.store.Key;

@Test
public class EncryptionHelperTest {
    public void testEncryptedIdentifiers() throws Exception {
        String[] encrypted = new String[] { "foo", "bar", "baz" };

        Random random = new Random(0L);

        for (String enc : encrypted) {
            for (int i = 0; i < 100; i++) {
                Key key1 = new Key(enc, random.nextLong());
                Key key2 = new Key(enc, key1.getId());
                Assert.assertEquals(
                        KeyEncryptionHelper.decrypt(key1.getEncryptedIdentifier())
                                .getIdentifier(), key1.getIdentifier());
                Assert.assertEquals(
                        KeyEncryptionHelper.decrypt(key2.getEncryptedIdentifier())
                                .getIdentifier(), key1.getIdentifier());
                Assert.assertNotSame(key1.getEncryptedIdentifier(),
                        key2.getEncryptedIdentifier());
            }
        }
    }
}
