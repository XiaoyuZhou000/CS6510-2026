package unit.common;

import io.grpc.Status;
import org.junit.jupiter.api.Test;
import rpc.RpcStatusClassifier;
import rpc.RpcStatusClassifier.Kind;

import java.util.EnumSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Every status in the contracts/grpc-internal-contracts.md table, plus the unlisted ones. */
class RpcStatusClassifierTest {
    @Test
    void classifiesEveryContractStatus() {
        Map<Status.Code, Kind> contract = Map.of(
                Status.Code.OK, Kind.DEFINITIVE,
                Status.Code.INVALID_ARGUMENT, Kind.INVALID_ARGUMENT,
                Status.Code.FAILED_PRECONDITION, Kind.FAILED_PRECONDITION,
                Status.Code.UNAVAILABLE, Kind.UNCERTAIN,
                Status.Code.DEADLINE_EXCEEDED, Kind.UNCERTAIN,
                Status.Code.ABORTED, Kind.UNCERTAIN,
                Status.Code.RESOURCE_EXHAUSTED, Kind.UNCERTAIN,
                Status.Code.CANCELLED, Kind.UNCERTAIN,
                Status.Code.UNKNOWN, Kind.UNCERTAIN,
                Status.Code.INTERNAL, Kind.UNCERTAIN);
        contract.forEach((code, kind) -> assertEquals(kind, RpcStatusClassifier.classify(code), code.name()));
    }

    @Test
    void unlistedStatusesAreUncertainSoTheyNeverBecomeBusinessOutcomes() {
        EnumSet<Status.Code> definitive = EnumSet.of(Status.Code.OK, Status.Code.INVALID_ARGUMENT,
                Status.Code.FAILED_PRECONDITION);
        for (Status.Code code : Status.Code.values()) {
            if (!definitive.contains(code)) {
                assertEquals(Kind.UNCERTAIN, RpcStatusClassifier.classify(code), code.name());
                assertTrue(RpcStatusClassifier.isUncertain(code));
            } else {
                assertFalse(RpcStatusClassifier.isUncertain(code));
            }
        }
        assertEquals(Kind.UNCERTAIN, RpcStatusClassifier.classify((Status.Code) null));
    }

    @Test
    void classifiesCallFailuresByTheirStatus() {
        assertEquals(Kind.FAILED_PRECONDITION,
                RpcStatusClassifier.classify(Status.FAILED_PRECONDITION.withDescription("mismatch").asRuntimeException()));
        assertEquals(Kind.UNCERTAIN, RpcStatusClassifier.classify(Status.DEADLINE_EXCEEDED.asException()));
        assertEquals(Kind.UNCERTAIN, RpcStatusClassifier.classify(new IllegalStateException("no status")));
    }
}
