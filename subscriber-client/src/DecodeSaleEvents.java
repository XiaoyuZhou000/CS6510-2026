import selfcheckout.events.v1.EventEnvelope;
import selfcheckout.events.v1.SaleCompleted;
import selfcheckout.events.v1.SoldLine;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Decodes the exported {@code SaleCompleted} outbox rows for {@code db/export-notification-evidence.ps1} (T076).
 *
 * <p>Input ({@code --in}): the tab-separated {@code mysql --batch} output of {@code db/transaction/export-sale-events.sql},
 * with the header {@code event_id, aggregate_key, published_at_ms, envelope_hex}. Output ({@code --out}):
 * {@code sale-events.tsv} with {@code event_id, aggregate_key, published_at_ms, sale_id, completed_at_ms, lines},
 * where {@code lines} is the canonical {@code [{"sku","quantity"}]} JSON of the event. A row that cannot be decoded
 * keeps its identity with an empty {@code sale_id} and {@code lines = UNDECODABLE: <reason>}, which fails the
 * reconciler's sale-event check.
 */
public final class DecodeSaleEvents {
    static final String HEADER = "event_id\taggregate_key\tpublished_at_ms\tsale_id\tcompleted_at_ms\tlines";

    private DecodeSaleEvents() { }

    public static void main(String[] args) throws IOException {
        Path in = null;
        Path out = null;
        for (String arg : args) {
            if (arg.startsWith("--in=")) in = Path.of(arg.substring(5));
            else if (arg.startsWith("--out=")) out = Path.of(arg.substring(6));
            else {
                System.err.println("Usage: DecodeSaleEvents --in=FILE --out=FILE");
                System.exit(2);
            }
        }
        if (in == null || out == null) {
            System.err.println("Usage: DecodeSaleEvents --in=FILE --out=FILE");
            System.exit(2);
        }
        int[] counts = decode(in, out);
        System.out.println("decoded " + counts[0] + " SaleCompleted rows (" + counts[1] + " undecodable) into " + out);
    }

    /** Returns {decoded rows, undecodable rows}. */
    static int[] decode(Path in, Path out) throws IOException {
        List<Map<String, String>> rows = RunData.readTsv(in);
        int undecodable = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
            writer.write(HEADER);
            writer.write('\n');
            for (Map<String, String> row : rows) {
                String saleId = "";
                String completedAt = "";
                String lines;
                try {
                    EventEnvelope envelope = EventEnvelope.parseFrom(HexFormat.of().parseHex(row.get("envelope_hex")));
                    if (!"SaleCompleted".equals(envelope.getEventType())) {
                        throw new IllegalArgumentException("event type " + envelope.getEventType());
                    }
                    SaleCompleted sale = SaleCompleted.parseFrom(envelope.getPayload());
                    saleId = sale.getSaleId();
                    completedAt = Long.toString(sale.getCompletedAtMs());
                    List<Object> decoded = new ArrayList<>();
                    for (SoldLine line : sale.getLinesList()) {
                        decoded.add(Json.object("sku", line.getSku(), "quantity", (long) line.getQuantity()));
                    }
                    lines = Json.compact(decoded);
                } catch (IOException | RuntimeException e) {
                    undecodable++;
                    lines = "UNDECODABLE: " + e.getClass().getSimpleName();
                }
                writer.write(String.join("\t", field(row.get("event_id")), field(row.get("aggregate_key")),
                        field(row.get("published_at_ms")), saleId, completedAt, lines));
                writer.write('\n');
            }
        }
        return new int[] {rows.size(), undecodable};
    }

    private static String field(String value) {
        return value == null ? "NULL" : value.replace("\t", " ").replace("\n", " ");
    }
}
