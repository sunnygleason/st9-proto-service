package utest.com.g414.st9.proto.service.store;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.helper.EncodingHelper;
import com.g414.st9.proto.service.store.Key;

@Test
public class EncodingHelperTest {
    public void testKVCacheKeyEncoding() throws Exception {
        Random random = new Random(0L);

        for (int i = 0; i < 1000; i++) {
            Key key1 = new Key("foo", random.nextLong());
            String cacheKey1 = EncodingHelper
                    .toKVCacheKey(key1.getIdentifier());

            Assert.assertEquals(EncodingHelper.fromKVCacheKey(cacheKey1),
                    key1.getIdentifier());
        }
    }

    public void testUniqueIndexCacheKeyEncoding() throws Exception {
        Random random = new Random(0L);

        for (int i = 0; i < 1000; i++) {
            Key key1 = new Key("foo", random.nextLong());
            List<String> theList = Arrays.asList(key1.getIdentifier());
            String cacheKey1 = EncodingHelper.toUniqueIdxCacheKey(theList);

            Assert.assertEquals(
                    EncodingHelper.fromUniqueIdxCacheKey(cacheKey1),
                    Arrays.asList(key1.getIdentifier()));
        }

        for (int i = 0; i < 1000; i++) {
            Key key1 = new Key("foo", random.nextLong());
            List<String> theList = Arrays.asList(key1.getEncryptedIdentifier(),
                    key1.getIdentifier(), key1.getId().toString());

            String cacheKey1 = EncodingHelper.toUniqueIdxCacheKey(theList);

            Assert.assertEquals(
                    EncodingHelper.fromUniqueIdxCacheKey(cacheKey1), theList);
        }
    }
}
