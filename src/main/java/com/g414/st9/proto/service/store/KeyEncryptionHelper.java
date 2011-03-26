package com.g414.st9.proto.service.store;

import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Random;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Hex;

public class KeyEncryptionHelper {
    private static Random random = new Random();

    private static char[] keyChars = System.getProperty("encrypt.password",
            "changeme").toCharArray();
    private static byte[] saltChars = System.getProperty("encrypt.salt",
            "asalt").getBytes();
    private static final AlgorithmParameterSpec paramSpec = new IvParameterSpec(
            new byte[16]);

    public static String encrypt(String type, Long id) throws Exception {
        StringBuilder encryptedIdentifier = new StringBuilder();
        encryptedIdentifier.append("@");
        encryptedIdentifier.append(type);
        encryptedIdentifier.append(":");

        byte[] rand = new byte[7];
        random.nextBytes(rand);

        byte[] plain = ByteBuffer.allocate(15).put(rand).putLong(id).array();
        byte[] encrypted = getCipher(Cipher.ENCRYPT_MODE).doFinal(plain);

        encryptedIdentifier.append(Hex.encodeHexString(encrypted));

        return encryptedIdentifier.toString();
    }

    public static Key decrypt(String encryptedText) throws Exception {
        if (!encryptedText.startsWith("@")) {
            return Key.valueOf(encryptedText);
        }

        String[] parts = encryptedText.substring(1).split(":");
        byte[] encrypted = Hex.decodeHex(parts[1].toCharArray());
        byte[] decrypted = getCipher(Cipher.DECRYPT_MODE).doFinal(encrypted);

        Long id = ByteBuffer.allocate(8).put(decrypted, 7, 8).getLong(0);

        return new Key(parts[0], id);
    }

    private static Cipher getCipher(int mode) throws NoSuchAlgorithmException,
            NoSuchPaddingException, InvalidKeyException,
            InvalidAlgorithmParameterException, InvalidKeySpecException {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(mode, getKey(), paramSpec);
        return cipher;
    }

    private static SecretKey getKey() throws NoSuchAlgorithmException,
            InvalidKeySpecException {
        SecretKeyFactory factory = SecretKeyFactory
                .getInstance("PBKDF2WithHmacSHA1");
        KeySpec spec = new PBEKeySpec(keyChars, saltChars, 1024, 256);
        SecretKey tmp = factory.generateSecret(spec);
        SecretKey key = new SecretKeySpec(tmp.getEncoded(), "AES");
        return key;
    }
}
