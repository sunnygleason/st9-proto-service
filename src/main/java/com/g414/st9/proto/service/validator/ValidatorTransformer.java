package com.g414.st9.proto.service.validator;

/**
 * Interface for transforming and validating values for KV storage.
 * 
 * @param <T>
 * @param <V>
 */
public interface ValidatorTransformer<T, V> {
    public V validateTransform(T instance) throws ValidationException;

    public T untransform(V instance) throws ValidationException;
}
