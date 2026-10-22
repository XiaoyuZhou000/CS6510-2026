package transaction;

/**
 * A peer service could not give a definitive answer: it was unreachable, timed out, or failed in a way
 * that may or may not have taken effect. Callers retry under the same identity; it is never a business
 * rejection, and it maps to 500 at the public API.
 */
public final class DependencyUnavailableFailure extends RuntimeException {
    public DependencyUnavailableFailure(String message) {
        super(message);
    }

    public DependencyUnavailableFailure(String message, Throwable cause) {
        super(message, cause);
    }
}
