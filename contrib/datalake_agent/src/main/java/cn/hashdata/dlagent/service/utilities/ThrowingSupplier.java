package cn.hashdata.dlagent.service.utilities;

/**
 * Functional interface for a supplier function that can throw an exception.
 *
 * @param <T> type of the return data
 * @param <E> type of the exception that can be thrown
 */
@FunctionalInterface
public interface ThrowingSupplier<T, E extends Exception> {
    T get() throws E;
}
