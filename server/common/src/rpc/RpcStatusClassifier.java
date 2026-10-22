package rpc;

import io.grpc.Status;

/**
 * Caller-side meaning of a gRPC status, per contracts/grpc-internal-contracts.md.
 *
 * <p>Only {@code OK}, {@code INVALID_ARGUMENT}, and {@code FAILED_PRECONDITION} are definitive.
 * Every other status, including codes the contract table does not list, is {@link Kind#UNCERTAIN}:
 * the server may have committed, so the caller retries under the same identity and never treats
 * the failure as a business rejection.</p>
 */
public final class RpcStatusClassifier {
    public enum Kind { DEFINITIVE, INVALID_ARGUMENT, FAILED_PRECONDITION, UNCERTAIN }

    private RpcStatusClassifier() { }

    public static Kind classify(Status.Code code) {
        if (code == null) return Kind.UNCERTAIN;
        return switch (code) {
            case OK -> Kind.DEFINITIVE;
            case INVALID_ARGUMENT -> Kind.INVALID_ARGUMENT;
            case FAILED_PRECONDITION -> Kind.FAILED_PRECONDITION;
            default -> Kind.UNCERTAIN;
        };
    }

    /** Classifies a failed call; a failure that carries no gRPC status is uncertain. */
    public static Kind classify(Throwable failure) {
        return classify(Status.fromThrowable(failure).getCode());
    }

    public static boolean isUncertain(Status.Code code) {
        return classify(code) == Kind.UNCERTAIN;
    }
}
