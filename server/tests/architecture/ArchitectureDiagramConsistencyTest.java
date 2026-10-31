package architecture;

import events.EventTypes;
import events.TopicNames;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The committed architecture diagram agrees with the event catalog (FR-063, D16, SC-015). The Mermaid source in
 * {@code docs/architecture/event-based-architecture.md} must show the gateway and the three services with the
 * ports of {@code deploy/nginx.conf}, the three schemas with their outbox and dead-letter tables from
 * {@code db/<service>/schema.sql}, the broker with the four topics of {@link TopicNames}, one producer edge and one
 * consumer edge per event type of {@link EventTypes}, and the consumer groups of the event catalog. Every edge must
 * be one of the allowed kinds, so no edge connects two services or a service to another service's schema. The
 * exported SVG must exist, name every topic and group, and be at least as recent as the Mermaid source.
 */
class ArchitectureDiagramConsistencyTest {
    private static final String DIAGRAM = "docs/architecture/event-based-architecture.md";
    private static final String SVG = "docs/architecture/event-based-architecture.svg";
    private static final List<String> SERVICES = List.of(EventTypes.TRANSACTION, EventTypes.INVENTORY, "analytics");
    private static final Pattern NODE = Pattern.compile("^(\\w+)\\s*[\\[({]+\"(.*)\"[\\])}]+$");
    private static final Pattern SUBGRAPH = Pattern.compile("^subgraph\\s+(\\w+)\\s*\\[\"(.*)\"]$");
    private static final Pattern EDGE = Pattern.compile("^(\\w+)\\s*(?:-->|-\\.->|==>)\\s*(?:\\|\"(.*?)\"\\|\\s*)?(\\w+)$");
    private static final Pattern UPSTREAM = Pattern.compile("upstream\\s+(\\w+)_service\\s*\\{\\s*server\\s+[\\d.]+:(\\d+);");
    private static final Pattern LISTEN = Pattern.compile("\\blisten\\s+(\\d+);");
    private static final Pattern CREATE_TABLE = Pattern.compile("(?m)^CREATE TABLE `?(\\w+)`?");
    private static final Pattern MERMAID_CLI = Pattern.compile("@mermaid-js/mermaid-cli@\\d+\\.\\d+\\.\\d+");

    record Node(String id, String label, String parent) {}

    record Edge(String from, String to, String label, int line) {}

    /** The parsed {@code flowchart}: nodes and subgraphs by ID, with each one's enclosing subgraph, and the edges. */
    record Diagram(Map<String, Node> nodes, List<Edge> edges) {
        /** The node or subgraph whose label contains {@code text}, ignoring case; exactly one must match. */
        Node labelled(String text) {
            List<Node> matches = nodes.values().stream()
                    .filter(node -> plain(node.label()).toLowerCase(Locale.ROOT).contains(text.toLowerCase(Locale.ROOT)))
                    .toList();
            assertEquals(1, matches.size(), "exactly one node of the diagram is labelled with \"" + text + "\": " + matches);
            return matches.get(0);
        }

        boolean within(Node node, Node ancestor) {
            for (String parent = node.parent(); parent != null; parent = nodes.get(parent).parent()) {
                if (parent.equals(ancestor.id())) return true;
            }
            return false;
        }

        List<Edge> between(Node from, Node to) {
            return edges.stream().filter(edge -> edge.from().equals(from.id()) && edge.to().equals(to.id())).toList();
        }
    }

    @Test
    void theDiagramShowsTheGatewayServicesSchemasBrokerAndTopics() throws Exception {
        Diagram diagram = diagram();
        Map<String, String> ports = servicePorts();
        Node gateway = diagram.labelled("Nginx");
        assertTrue(plain(gateway.label()).contains(ports.get("gateway")), "the gateway shows port " + ports.get("gateway"));
        for (String service : SERVICES) {
            Node node = serviceNode(diagram, service);
            assertTrue(plain(node.label()).contains(ports.get(service)), node.label() + " shows port " + ports.get(service));
            Node schema = diagram.labelled(schemaName(service));
            for (String table : eventTables(service)) {
                Node tableNode = tableNode(diagram, schema, table);
                assertNotNull(tableNode, schemaName(service) + " shows its " + table + " table");
            }
        }
        Node broker = diagram.labelled("Kafka");
        for (String topic : TopicNames.unprefixed().all()) {
            Node node = diagram.labelled(topic);
            assertTrue(diagram.within(node, broker), topic + " is drawn inside the broker");
        }
    }

    /**
     * Every event type has an edge from its producer service to its topic and from that topic to the topic's
     * consumer service, both labelled with the type; each consumer edge names the topic's consumer group or mode
     * from the event catalog's "Topics" table, whose groups are those of {@link TopicNames}.
     */
    @Test
    void everyProducerAndConsumerGroupEdgeOfTheEventCatalogIsDrawn() throws Exception {
        Diagram diagram = diagram();
        Map<String, String[]> topics = catalogTopics();
        assertEquals(new TreeSet<>(TopicNames.unprefixed().all()), new TreeSet<>(topics.keySet()),
                "the event catalog lists the topics of TopicNames");
        Set<String> groups = new TreeSet<>();
        for (String[] row : topics.values()) if (!row[2].isEmpty()) groups.add(row[2]);
        assertEquals(new TreeSet<>(Set.of(TopicNames.INVENTORY_CHECKOUT_GROUP, TopicNames.TRANSACTION_CHECKOUT_OUTCOME_GROUP,
                TopicNames.ANALYTICS_SCAN_INGEST_GROUP)), groups, "the event catalog names the consumer groups of TopicNames");
        List<String> violations = new ArrayList<>();
        for (EventTypes type : EventTypes.values()) {
            String topic = TopicNames.unprefixed().name(type.topic());
            String[] row = topics.get(topic);
            if (!row[0].equals(type.producer())) violations.add(type + ": catalog producer " + row[0]);
            Node topicNode = diagram.labelled(topic);
            Node producer = serviceNode(diagram, type.producer());
            Node consumer = serviceNode(diagram, row[1]);
            if (diagram.between(producer, topicNode).stream().noneMatch(edge -> plain(edge.label()).contains(type.typeName()))) {
                violations.add("no edge " + type.producer() + " --> " + topic + " labelled " + type.typeName());
            }
            List<Edge> consumed = diagram.between(topicNode, consumer);
            if (consumed.stream().noneMatch(edge -> plain(edge.label()).contains(type.typeName()))) {
                violations.add("no edge " + topic + " --> " + row[1] + " labelled " + type.typeName());
            }
            String mode = row[2].isEmpty() ? "manual assignment" : row[2];
            if (consumed.stream().noneMatch(edge -> plain(edge.label()).contains(mode))) {
                violations.add("the edge " + topic + " --> " + row[1] + " does not name " + mode);
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    /**
     * Every edge is a client request to the gateway, a gateway route to a service, a service's use of its own
     * schema, a catalog producer edge, or a catalog consumer edge. Nothing else may be drawn, in particular no
     * edge between two services and none from a service to another service's schema (FR-063).
     */
    @Test
    void noEdgeConnectsTwoServicesOrContradictsTheCatalog() throws Exception {
        Diagram diagram = diagram();
        Map<String, String[]> topics = catalogTopics();
        Node gateway = diagram.labelled("Nginx");
        Map<String, String> serviceOf = new LinkedHashMap<>();
        Map<String, String> schemaOf = new LinkedHashMap<>();
        for (String service : SERVICES) {
            serviceOf.put(serviceNode(diagram, service).id(), service);
            Node schema = diagram.labelled(schemaName(service));
            schemaOf.put(schema.id(), service);
            for (Node node : diagram.nodes().values()) if (diagram.within(node, schema)) schemaOf.put(node.id(), service);
        }
        Map<String, String> topicOf = new LinkedHashMap<>();
        for (String topic : topics.keySet()) topicOf.put(diagram.labelled(topic).id(), topic);
        List<String> violations = new ArrayList<>();
        int routes = 0;
        for (Edge edge : diagram.edges()) {
            String from = edge.from();
            String to = edge.to();
            String where = "line " + edge.line() + ": " + from + " --> " + to;
            if (serviceOf.containsKey(from) && serviceOf.containsKey(to)) {
                violations.add(where + " connects two services directly");
            } else if (to.equals(gateway.id())) {
                if (diagram.nodes().get(from) == null || serviceOf.containsKey(from) || schemaOf.containsKey(from)
                        || topicOf.containsKey(from)) {
                    violations.add(where + ": only a client may call the gateway");
                }
            } else if (from.equals(gateway.id())) {
                if (!serviceOf.containsKey(to)) violations.add(where + ": the gateway routes only to services");
                routes++;
            } else if (serviceOf.containsKey(from) && schemaOf.containsKey(to)) {
                if (!serviceOf.get(from).equals(schemaOf.get(to))) violations.add(where + " reaches another service's schema");
            } else if (serviceOf.containsKey(from) && topicOf.containsKey(to)) {
                if (!topics.get(topicOf.get(to))[0].equals(serviceOf.get(from))) {
                    violations.add(where + ": the catalog names another producer of " + topicOf.get(to));
                }
            } else if (topicOf.containsKey(from) && serviceOf.containsKey(to)) {
                if (!topics.get(topicOf.get(from))[1].equals(serviceOf.get(to))) {
                    violations.add(where + ": the catalog names another consumer of " + topicOf.get(from));
                }
            } else {
                violations.add(where + " is not a gateway, database, producer, or consumer edge");
            }
        }
        assertEquals(SERVICES.size(), routes, "the gateway routes to each service once");
        for (String service : SERVICES) {
            Node schema = diagram.labelled(schemaName(service));
            boolean uses = diagram.edges().stream().anyMatch(edge -> edge.from().equals(serviceNode(diagram, service).id())
                    && service.equals(schemaOf.get(edge.to())));
            if (!uses) violations.add(service + " has no edge to its schema " + schema.label());
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    /**
     * The SVG export exists, names every topic and consumer group, and is at least as recent as the Mermaid source:
     * by last commit when both are committed unchanged, otherwise by file time. The Markdown records the pinned
     * Mermaid CLI version, links the event catalog, and the root README links both files.
     */
    @Test
    void theSvgExportIsPresentCurrentAndLinked() throws Exception {
        Path root = repositoryRoot();
        Path markdown = root.resolve(DIAGRAM);
        Path svg = root.resolve(SVG);
        assertTrue(Files.isRegularFile(svg), "Missing " + svg);
        String image = Files.readString(svg, StandardCharsets.UTF_8);
        assertTrue(image.contains("<svg"), SVG + " is an SVG document");
        for (String topic : TopicNames.unprefixed().all()) assertTrue(image.contains(topic), SVG + " names " + topic);
        for (String group : List.of(TopicNames.INVENTORY_CHECKOUT_GROUP, TopicNames.TRANSACTION_CHECKOUT_OUTCOME_GROUP,
                TopicNames.ANALYTICS_SCAN_INGEST_GROUP)) {
            assertTrue(image.contains(group), SVG + " names " + group);
        }
        long source = lastChange(root, DIAGRAM);
        long export = lastChange(root, SVG);
        assertTrue(export >= source, SVG + " (" + export + ") is older than " + DIAGRAM + " (" + source + "); export it again");
        String text = Files.readString(markdown, StandardCharsets.UTF_8);
        assertTrue(MERMAID_CLI.matcher(text).find(), DIAGRAM + " records the pinned Mermaid CLI version");
        assertTrue(text.contains("event-catalog.md"), DIAGRAM + " links the event catalog");
        String readme = Files.readString(root.resolve("README.md"), StandardCharsets.UTF_8);
        assertTrue(readme.contains(DIAGRAM), "README.md links " + DIAGRAM);
        assertTrue(readme.contains(SVG), "README.md links " + SVG);
    }

    private static Node serviceNode(Diagram diagram, String service) {
        return diagram.labelled(Character.toUpperCase(service.charAt(0)) + service.substring(1) + " Service");
    }

    /** The node inside {@code schema} labelled with {@code table} as a whole word. */
    private static Node tableNode(Diagram diagram, Node schema, String table) {
        Pattern word = Pattern.compile("\\b" + table + "\\b");
        return diagram.nodes().values().stream()
                .filter(node -> diagram.within(node, schema) && word.matcher(plain(node.label())).find())
                .findFirst().orElse(null);
    }

    private static String schemaName(String service) {
        return "cs6510_" + service;
    }

    /** The service's {@code outbox_event}, {@code dead_letter_event}, and {@code consumer_offset}, as its schema has them. */
    private static List<String> eventTables(String service) throws IOException {
        String schema = Files.readString(repositoryRoot().resolve("db/" + service + "/schema.sql"), StandardCharsets.UTF_8);
        Set<String> tables = new TreeSet<>();
        Matcher matcher = CREATE_TABLE.matcher(schema);
        while (matcher.find()) tables.add(matcher.group(1));
        assertTrue(tables.contains("dead_letter_event"), service + " has a dead_letter_event table");
        return List.of("outbox_event", "dead_letter_event", "consumer_offset").stream().filter(tables::contains).toList();
    }

    /** The gateway's listen port and each service's upstream port, from {@code deploy/nginx.conf}. */
    private static Map<String, String> servicePorts() throws IOException {
        String config = Files.readString(repositoryRoot().resolve("deploy/nginx.conf"), StandardCharsets.UTF_8);
        Map<String, String> ports = new LinkedHashMap<>();
        Matcher listen = LISTEN.matcher(config);
        assertTrue(listen.find(), "nginx.conf listens on a port");
        ports.put("gateway", listen.group(1));
        Matcher upstream = UPSTREAM.matcher(config);
        while (upstream.find()) ports.put(upstream.group(1), upstream.group(2));
        for (String service : SERVICES) assertTrue(ports.containsKey(service), "nginx.conf has an upstream for " + service);
        return ports;
    }

    /** {@code topic -> [producer service, consumer service, group or ""]} from the event catalog's "Topics" table. */
    private static Map<String, String[]> catalogTopics() throws IOException {
        Path file = repositoryRoot().resolve("specs/006-event-based-architecture/contracts/event-catalog.md");
        Map<String, String[]> rows = new LinkedHashMap<>();
        boolean inTable = false;
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.startsWith("## ")) inTable = line.equals("## Topics");
            if (!inTable || !line.startsWith("| `")) continue;
            String[] cells = line.split("\\|");
            String topic = cells[1].trim().replace("`", "");
            String producer = cells[5].trim().toLowerCase(Locale.ROOT);
            String consumerCell = cells[6].trim();
            String consumer = consumerCell.split("\\s+")[0].toLowerCase(Locale.ROOT);
            Matcher group = Pattern.compile("`([^`]+)`").matcher(consumerCell);
            rows.put(topic, new String[] {producer, consumer, group.find() ? group.group(1) : ""});
        }
        assertFalse(rows.isEmpty(), "No topics found in " + file);
        return rows;
    }

    /** Parses the first {@code mermaid} block of the diagram page: node, subgraph, and edge lines. */
    private static Diagram diagram() throws IOException {
        Path file = repositoryRoot().resolve(DIAGRAM);
        assertTrue(Files.isRegularFile(file), "Missing " + file);
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        int start = lines.indexOf("```mermaid");
        assertTrue(start >= 0, DIAGRAM + " has a mermaid block");
        Map<String, Node> nodes = new LinkedHashMap<>();
        List<Edge> edges = new ArrayList<>();
        List<String> stack = new ArrayList<>();
        boolean header = false;
        for (int index = start + 1; index < lines.size(); index++) {
            String line = lines.get(index).trim();
            if (line.equals("```")) break;
            if (line.isEmpty() || line.startsWith("%%") || line.startsWith("classDef ") || line.startsWith("class ")
                    || line.startsWith("style ") || line.startsWith("linkStyle ") || line.startsWith("direction ")) {
                continue;
            }
            if (!header) {
                assertTrue(line.startsWith("flowchart "), DIAGRAM + " is a Mermaid flowchart: " + line);
                header = true;
                continue;
            }
            String parent = stack.isEmpty() ? null : stack.get(stack.size() - 1);
            Matcher subgraph = SUBGRAPH.matcher(line);
            Matcher node = NODE.matcher(line);
            Matcher edge = EDGE.matcher(line);
            if (subgraph.matches()) {
                define(nodes, new Node(subgraph.group(1), subgraph.group(2), parent));
                stack.add(subgraph.group(1));
            } else if (line.equals("end")) {
                assertFalse(stack.isEmpty(), DIAGRAM + " line " + (index + 1) + ": end without subgraph");
                stack.remove(stack.size() - 1);
            } else if (edge.matches()) {
                edges.add(new Edge(edge.group(1), edge.group(3), edge.group(2) == null ? "" : edge.group(2), index + 1));
            } else if (node.matches()) {
                define(nodes, new Node(node.group(1), node.group(2), parent));
            } else {
                throw new AssertionError(DIAGRAM + " line " + (index + 1) + " is not a node, subgraph, or edge: " + line);
            }
        }
        assertTrue(stack.isEmpty(), DIAGRAM + " closes every subgraph");
        for (Edge edge : edges) {
            assertTrue(nodes.containsKey(edge.from()) && nodes.containsKey(edge.to()),
                    "line " + edge.line() + " connects an undefined node: " + edge);
        }
        return new Diagram(nodes, edges);
    }

    private static void define(Map<String, Node> nodes, Node node) {
        assertFalse(nodes.containsKey(node.id()), DIAGRAM + " defines " + node.id() + " twice");
        nodes.put(node.id(), node);
    }

    /** A label without Mermaid's HTML line breaks and markup. */
    private static String plain(String label) {
        return label.replaceAll("<br\\s*/?>", " ").replaceAll("<[^>]+>", "").replace("&nbsp;", " ");
    }

    /**
     * When the file was last changed, in epoch milliseconds: its last commit time if git reports the file tracked
     * and unchanged, otherwise its modification time.
     */
    private static long lastChange(Path root, String file) throws Exception {
        long modified = Files.getLastModifiedTime(root.resolve(file)).toMillis();
        String status = git(root, "status", "--porcelain", "--", file);
        if (status == null || !status.isBlank()) return modified;
        String committed = git(root, "log", "-1", "--format=%ct", "--", file);
        if (committed == null || committed.isBlank()) return modified;
        return Long.parseLong(committed.trim()) * 1000;
    }

    /** The command's standard output, or null when git is unavailable or fails. */
    private static String git(Path root, String... arguments) throws InterruptedException {
        List<String> command = new ArrayList<>(List.of("git", "-C", root.toString()));
        command.addAll(List.of(arguments));
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(false).start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                return null;
            }
            return process.exitValue() == 0 ? output : null;
        } catch (IOException missing) {
            return null;
        }
    }

    private static Path repositoryRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("server/common")) ? cwd : cwd.getParent();
    }
}
