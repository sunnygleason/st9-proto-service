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
            for (int i = 0; i < 1000; i++) {
                Key key1 = new Key(enc, random.nextLong());
                Key key2 = new Key(enc, key1.getId());

                Assert.assertEquals(
                        KeyEncryptionHelper.decrypt(
                                key1.getEncryptedIdentifier()).getIdentifier(),
                        key1.getIdentifier());
                Assert.assertEquals(
                        KeyEncryptionHelper.decrypt(
                                key2.getEncryptedIdentifier()).getIdentifier(),
                        key1.getIdentifier());
                Assert.assertEquals(key1.getEncryptedIdentifier(),
                        key2.getEncryptedIdentifier());
            }
        }
    }

    public void testActualEncryptedIdentifiers() throws Exception {
        Assert.assertEquals((new Key("foo", 1L)).getEncryptedIdentifier(),
                "@foo:190272f987c6ac27");
        Assert.assertEquals((new Key("foo", 2L)).getEncryptedIdentifier(),
                "@foo:ce4ad6a1cd6293d9");
        Assert.assertEquals((new Key("foo", 3L)).getEncryptedIdentifier(),
                "@foo:573c812fe6841168");

        Assert.assertEquals((new Key("bar", 1L)).getEncryptedIdentifier(),
                "@bar:75d1f4ee603cb168");
        Assert.assertEquals((new Key("bar", 2L)).getEncryptedIdentifier(),
                "@bar:13b44d644e215f80");
        Assert.assertEquals((new Key("bar", 3L)).getEncryptedIdentifier(),
                "@bar:0574f68854a546e2");
    }
}
