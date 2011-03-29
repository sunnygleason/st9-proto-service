package utest.com.g414.st9.proto.service.store;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.store.EncodingHelper;
import com.g414.st9.proto.service.store.Key;

@Test
public class EncodingHelperTest {
    public void testCacheKeyEncoding() throws Exception {
        Random random = new Random(0L);

        for (int i = 0; i < 1000; i++) {
            Key key1 = new Key("foo", random.nextLong());
            String cacheKey1 = EncodingHelper
                    .toKVCacheKey(key1.getIdentifier());

            System.out.println(key1.getIdentifier() + " " + cacheKey1);

            Assert.assertEquals(EncodingHelper.fromKVCacheKey(cacheKey1),
                    key1.getIdentifier());
        }
    }
}
