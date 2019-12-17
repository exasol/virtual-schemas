package com.exasol.reflect;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This class contains helper methods to reduce the overhead for accessing class members via reflection. This code is
 * targeted at making tests more compact and readable and should not be used in production code.
 */
public final class ReflectionUtils {
    private ReflectionUtils() {
        // prevent instantiation
    }

    /**
     *
     * @param object     instance on which the method is invoked
     * @param methodName name of the method to be invoked
     * @return resulting return value of the method invocation
     * @throws ReflectionException if the method does not exist or is inaccessible
     */
    public static Object getMethodReturnViaReflection(final Object object, final String methodName) {
        Method method;
        try {
            method = object.getClass().getDeclaredMethod(methodName);
            method.setAccessible(true);
            return method.invoke(object);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException exception) {
            throw new ReflectionException(exception);
        }
    }
}