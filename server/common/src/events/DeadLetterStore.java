package events;

/** A service's {@code dead_letter_event} table, the parked state of its consumers (D6, FR-013). */
public interface DeadLetterStore {
    /**
     * Records {@code record} in its own committed transaction. Parking a non-null {@code event_id} that is
     * already parked increments its {@code park_count} and {@code last_parked_at} instead of adding a row.
     */
    void park(ParkedRecord record);

    long countParked();
}
