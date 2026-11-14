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
 *
 * <p>007 (T118) adds the same checks for {@code docs/architecture/notification-service.md}: four services and their
 * schemas, the five fixed topics with every producer and consumer edge of the 006 catalog and 007's sale event
 * contract, the channel family {@code selfcheckout.notify.<subscriberId>.v1} from Notification to the subscriber test
 * application, and no synchronous service-to-service edge.</p>
 */
class ArchitectureDiagramConsistencyTest {
    private static final String DIAGRAM = "docs/architecture/event-based-architecture.md";
    private static final String SVG = "docs/architecture/event-based-architecture.svg";
    private static final List<String> SERVICES = List.of(EventTypes.TRANSACTION, EventTypes.INVENTORY, "analytics");
    /** 007 (T118): the Notification diagram, its export, its four services, and its two client-side nodes. */
    private static final String NOTIFICATION_DIAGRAM = "docs/architecture/notification-service.md";
    private static final String NOTIFICATION_SVG = "docs/architecture/notification-service.svg";
    private static final List<String> NOTIFICATION_SERVICES = List.of(EventTypes.TRANSACTION, EventTypes.INVENTORY,
            "analytics", EventTypes.NOTIFICATION);
    private static final String CHANNEL_FAMILY = "selfcheckout.notify.<subscriberId>.v1";
    private static final String SUBSCRIBER_APPLICATION = "Subscriber test application";
    private static final Pattern NODE = Pattern.compile("^(\\w+)\\s*[\\[({]+\"(.*)\"[\\])}]+$");
    private static final Pattern SUBGRAPH = Pattern.compile("^subgraph\\s+(\\w+)\\s*\\[\"(.*)\"]$");
    private static final Pattern EDGE = Pattern.compile("^(\\w+)\\s*(?:-->|-\\.->|==>)\\s*(?:\\|\"(.*?)\"\\|\\s*)?(\\w+)$");
    private static final Pattern UPSTREAM = Pattern.compile("upstream\\s+(\\w+)_service\\s*\\{\\s*server\\s+[\\d.]+:(\\d+);");
    private static final Pattern LISTEN = Pattern.compile("\\blisten\\s+(\\d+);");
    private static final Pattern CREATE_TABLE = Pattern.compile("(?m)^CREATE TABLE `?(\\w+)`?");
    private static final Pattern MERMAID_CLI = Pattern.compile("@mermaid-js/mermaid-cli@\\d+\\.\\d+\\.\\d+");

    /**
     * The 006 diagram's topics: those of {@link TopicNames} without 007's sale-completed topic, which the
     * Notification diagram draws (007 T118).
     */
    private static List<String> diagrammedTopics() {
        TopicNames names = TopicNames.unprefixed();
        return names.all().stream().filter(topic -> !topic.equals(names.saleCompleted())).toList();
    }

    /** The 006 diagram's event types: those on its topics, so neither of 007's two types (007 T118). */
    private static List<EventTypes> diagrammedTypes() {
        return java.util.Arrays.stream(EventTypes.values())
                .filter(type -> type.topic().filter(topic -> topic != TopicNames.Topic.SALE_COMPLETED).isPresent())
                .toList();
    }

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
        for (String topic : diagrammedTopics()) {
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
        assertEquals(new TreeSet<>(diagrammedTopics()), new TreeSet<>(topics.keySet()),
                "the event catalog lists the topics of TopicNames");
        Set<String> groups = new TreeSet<>();
        for (String[] row : topics.values()) if (!row[2].isEmpty()) groups.add(row[2]);
        assertEquals(new TreeSet<>(Set.of(TopicNames.INVENTORY_CHECKOUT_GROUP, TopicNames.TRANSACTION_CHECKOUT_OUTCOME_GROUP,
                TopicNames.ANALYTICS_SCAN_INGEST_GROUP)), groups, "the event catalog names the consumer groups of TopicNames");
        List<String> violations = new ArrayList<>();
        for (EventTypes type : diagrammedTypes()) {
            String topic = TopicNames.unprefixed().name(type.topic().orElseThrow());
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
        for (String topic : diagrammedTopics()) assertTrue(image.contains(topic), SVG + " names " + topic);
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
        return servicePorts(SERVICES);
    }

    private static Map<String, String> servicePorts(List<String> services) throws IOException {
        String config = Files.readString(repositoryRoot().resolve("deploy/nginx.conf"), StandardCharsets.UTF_8);
        Map<String, String> ports = new LinkedHashMap<>();
        Matcher listen = LISTEN.matcher(config);
        assertTrue(listen.find(), "nginx.conf listens on a port");
        ports.put("gateway", listen.group(1));
        Matcher upstream = UPSTREAM.matcher(config);
        while (upstream.find()) ports.put(upstream.group(1), upstream.group(2));
        for (String service : services) assertTrue(ports.containsKey(service), "nginx.conf has an upstream for " + service);
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

    /**
     * 007 (T118): {@code topic -> [producer, then for each consumer: consumer service, group or "manual assignment"]}
     * for the five fixed topics: the 006 event catalog's rows, plus the additions of 007's sale event contract (the
     * sale topic, and Notification as a second reader of the catalog topic). The channel family is not in it.
     */
    private static Map<String, List<String[]>> notificationTopics() throws IOException {
        Map<String, List<String[]>> topics = new LinkedHashMap<>();
        for (Map.Entry<String, String[]> row : catalogTopics().entrySet()) {
            String[] cells = row.getValue();
            topics.put(row.getKey(), new ArrayList<>(List.of(new String[] {cells[0]},
                    new String[] {cells[1], cells[2].isEmpty() ? "manual assignment" : cells[2]})));
        }
        Path file = repositoryRoot().resolve("specs/007-notification-service/contracts/sale-completed-event.md");
        boolean inTable = false;
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.startsWith("## ")) inTable = line.startsWith("## Topics");
            if (!inTable || !line.startsWith("| `")) continue;
            String[] cells = line.split("\\|");
            String topic = cells[1].trim().replaceAll("`|\\s*\\(.*", "");
            if (topic.contains(".notify.")) continue;
            String consumerCell = cells[6].trim().replace("**added**:", "").trim();
            String consumer = consumerCell.split("\\s+")[0].toLowerCase(Locale.ROOT);
            // A manual reader's cell may hold other code spans (such as `KnownSkus`), so it is recognized first.
            Matcher group = Pattern.compile("`([^`]+)`").matcher(consumerCell);
            String mode = consumerCell.contains("manual assignment") || !group.find() ? "manual assignment" : group.group(1);
            String[] reader = {consumer, mode};
            if (topics.containsKey(topic)) {
                topics.get(topic).add(reader);
            } else {
                topics.put(topic, new ArrayList<>(List.of(new String[] {cells[5].trim().toLowerCase(Locale.ROOT)}, reader)));
            }
        }
        return topics;
    }

    @Test
    void theNotificationDiagramShowsFourServicesTheirSchemasTheFiveTopicsAndTheChannels() throws Exception {
        Diagram diagram = diagram(NOTIFICATION_DIAGRAM);
        Map<String, String> ports = servicePorts(NOTIFICATION_SERVICES);
        Node gateway = diagram.labelled("Nginx");
        assertTrue(plain(gateway.label()).contains(ports.get("gateway")), "the gateway shows port " + ports.get("gateway"));
        for (String service : NOTIFICATION_SERVICES) {
            Node node = serviceNode(diagram, service);
            assertTrue(plain(node.label()).contains(ports.get(service)), node.label() + " shows port " + ports.get(service));
            Node schema = diagram.labelled(schemaName(service));
            for (String table : eventTables(service)) {
                assertNotNull(tableNode(diagram, schema, table), schemaName(service) + " shows its " + table + " table");
            }
        }
        Node broker = diagram.labelled("Kafka");
        TopicNames names = TopicNames.unprefixed();
        assertEquals(5, names.all().size(), "five fixed topics");
        assertEquals(new TreeSet<>(names.all()), new TreeSet<>(notificationTopics().keySet()),
                "the 006 catalog and 007's additions list the topics of TopicNames");
        for (String topic : names.all()) assertTrue(diagram.within(diagram.labelled(topic), broker), topic + " is inside the broker");
        Node channels = diagram.labelled(CHANNEL_FAMILY);
        assertTrue(diagram.within(channels, broker), "the channel family is inside the broker");
        assertTrue(names.channel("sub-0001").startsWith("selfcheckout.notify.") && names.channel("sub-0001").endsWith(".v1"),
                "TopicNames names channels as " + CHANNEL_FAMILY);
    }

    /**
     * Every event type of {@link EventTypes} on a fixed topic has its producer edge and an edge to each consumer of the
     * topic naming its group or mode; {@code SubscriberNotified} goes from Notification to the channel family, and the
     * channels to the subscriber test application, one thread per subscriber.
     */
    @Test
    void everyEventTypeOfTheNotificationDiagramHasItsProducerAndConsumerEdges() throws Exception {
        Diagram diagram = diagram(NOTIFICATION_DIAGRAM);
        Map<String, List<String[]>> topics = notificationTopics();
        Set<String> groups = new TreeSet<>();
        for (List<String[]> row : topics.values()) {
            for (String[] reader : row.subList(1, row.size())) if (!reader[1].equals("manual assignment")) groups.add(reader[1]);
        }
        assertEquals(new TreeSet<>(Set.of(TopicNames.INVENTORY_CHECKOUT_GROUP, TopicNames.TRANSACTION_CHECKOUT_OUTCOME_GROUP,
                TopicNames.ANALYTICS_SCAN_INGEST_GROUP, TopicNames.NOTIFICATION_SALE_COMPLETED_GROUP)), groups,
                "the contracts name the four consumer groups of TopicNames");
        List<String> violations = new ArrayList<>();
        for (EventTypes type : EventTypes.values()) {
            if (type.topic().isEmpty()) continue;
            String topic = TopicNames.unprefixed().name(type.topic().get());
            List<String[]> row = topics.get(topic);
            if (!row.get(0)[0].equals(type.producer())) violations.add(type + ": contract producer " + row.get(0)[0]);
            Node topicNode = diagram.labelled(topic);
            if (diagram.between(serviceNode(diagram, type.producer()), topicNode).stream()
                    .noneMatch(edge -> plain(edge.label()).contains(type.typeName()))) {
                violations.add("no edge " + type.producer() + " --> " + topic + " labelled " + type.typeName());
            }
            for (String[] reader : row.subList(1, row.size())) {
                List<Edge> consumed = diagram.between(topicNode, serviceNode(diagram, reader[0]));
                if (consumed.stream().noneMatch(edge -> plain(edge.label()).contains(type.typeName())
                        && plain(edge.label()).contains(reader[1]))) {
                    violations.add("no edge " + topic + " --> " + reader[0] + " labelled " + type.typeName() + " and " + reader[1]);
                }
            }
        }
        Node channels = diagram.labelled(CHANNEL_FAMILY);
        Node subscribers = diagram.labelled(SUBSCRIBER_APPLICATION);
        String notified = EventTypes.SUBSCRIBER_NOTIFIED.typeName();
        assertEquals(EventTypes.NOTIFICATION, EventTypes.SUBSCRIBER_NOTIFIED.producer());
        if (diagram.between(serviceNode(diagram, EventTypes.NOTIFICATION), channels).stream()
                .noneMatch(edge -> plain(edge.label()).contains(notified))) {
            violations.add("no edge notification --> the channel family labelled " + notified);
        }
        if (diagram.between(channels, subscribers).stream().noneMatch(edge -> plain(edge.label()).contains(notified)
                && plain(edge.label()).contains("one thread per subscriber"))) {
            violations.add("no edge from the channel family to the subscriber test application, one thread per subscriber");
        }
        assertTrue(plain(subscribers.label()).contains("N subscriber threads"), subscribers.label());
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    /**
     * Every edge of the 007 diagram is a client's request to the gateway (the Load Client, or the subscriber test
     * application registering), a gateway route, a service's use of its own schema, a producer or consumer edge of
     * the contracts, Notification's publishing to the channel family, or the channels' reading by the subscriber test
     * application. No edge connects two services or reaches another service's schema.
     */
    @Test
    void theNotificationDiagramHasNoSynchronousServiceToServiceEdge() throws Exception {
        Diagram diagram = diagram(NOTIFICATION_DIAGRAM);
        Map<String, List<String[]>> topics = notificationTopics();
        Node gateway = diagram.labelled("Nginx");
        Node channels = diagram.labelled(CHANNEL_FAMILY);
        Node subscribers = diagram.labelled(SUBSCRIBER_APPLICATION);
        Map<String, String> serviceOf = new LinkedHashMap<>();
        Map<String, String> schemaOf = new LinkedHashMap<>();
        for (String service : NOTIFICATION_SERVICES) {
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
                if (serviceOf.containsKey(from) || schemaOf.containsKey(from) || topicOf.containsKey(from) || from.equals(channels.id())) {
                    violations.add(where + ": only a client may call the gateway");
                }
            } else if (from.equals(gateway.id())) {
                if (!serviceOf.containsKey(to)) violations.add(where + ": the gateway routes only to services");
                routes++;
            } else if (serviceOf.containsKey(from) && schemaOf.containsKey(to)) {
                if (!serviceOf.get(from).equals(schemaOf.get(to))) violations.add(where + " reaches another service's schema");
            } else if (serviceOf.containsKey(from) && topicOf.containsKey(to)) {
                if (!topics.get(topicOf.get(to)).get(0)[0].equals(serviceOf.get(from))) {
                    violations.add(where + ": the contracts name another producer of " + topicOf.get(to));
                }
            } else if (topicOf.containsKey(from) && serviceOf.containsKey(to)) {
                List<String[]> readers = topics.get(topicOf.get(from));
                if (readers.subList(1, readers.size()).stream().noneMatch(reader -> reader[0].equals(serviceOf.get(to)))) {
                    violations.add(where + ": the contracts name no such consumer of " + topicOf.get(from));
                }
            } else if (to.equals(channels.id())) {
                if (!EventTypes.NOTIFICATION.equals(serviceOf.get(from))) violations.add(where + ": only Notification publishes channels");
            } else if (from.equals(channels.id())) {
                if (!to.equals(subscribers.id())) violations.add(where + ": only the subscriber test application reads channels");
            } else {
                violations.add(where + " is not a gateway, database, producer, consumer, or channel edge");
            }
        }
        assertEquals(NOTIFICATION_SERVICES.size(), routes, "the gateway routes to each of the four services once");
        assertTrue(diagram.edges().stream().anyMatch(edge -> edge.from().equals(subscribers.id()) && edge.to().equals(gateway.id())),
                "the subscriber test application registers through the gateway");
        for (String service : NOTIFICATION_SERVICES) {
            boolean uses = diagram.edges().stream().anyMatch(edge -> edge.from().equals(serviceNode(diagram, service).id())
                    && service.equals(schemaOf.get(edge.to())));
            if (!uses) violations.add(service + " has no edge to its schema");
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    /** The 007 SVG exists, names every topic, group, and the channel family, and is at least as recent as its page. */
    @Test
    void theNotificationSvgExportIsPresentAndCurrent() throws Exception {
        Path root = repositoryRoot();
        Path svg = root.resolve(NOTIFICATION_SVG);
        assertTrue(Files.isRegularFile(svg), "Missing " + svg);
        String image = Files.readString(svg, StandardCharsets.UTF_8);
        assertTrue(image.contains("<svg"), NOTIFICATION_SVG + " is an SVG document");
        for (String topic : TopicNames.unprefixed().all()) assertTrue(image.contains(topic), NOTIFICATION_SVG + " names " + topic);
        for (String group : List.of(TopicNames.INVENTORY_CHECKOUT_GROUP, TopicNames.TRANSACTION_CHECKOUT_OUTCOME_GROUP,
                TopicNames.ANALYTICS_SCAN_INGEST_GROUP, TopicNames.NOTIFICATION_SALE_COMPLETED_GROUP)) {
            assertTrue(image.contains(group), NOTIFICATION_SVG + " names " + group);
        }
        assertTrue(image.contains("selfcheckout.notify."), NOTIFICATION_SVG + " names the channel family");
        long source = lastChange(root, NOTIFICATION_DIAGRAM);
        long export = lastChange(root, NOTIFICATION_SVG);
        assertTrue(export >= source, NOTIFICATION_SVG + " is older than " + NOTIFICATION_DIAGRAM + "; export it again");
        String text = Files.readString(root.resolve(NOTIFICATION_DIAGRAM), StandardCharsets.UTF_8);
        assertTrue(MERMAID_CLI.matcher(text).find(), NOTIFICATION_DIAGRAM + " records the pinned Mermaid CLI version");
        assertTrue(text.contains("event-catalog.md") && text.contains("sale-completed-event.md"),
                NOTIFICATION_DIAGRAM + " links both event contracts");
    }

    /** The 006 diagram. */
    private static Diagram diagram() throws IOException {
        return diagram(DIAGRAM);
    }

    /** Parses the first {@code mermaid} block of {@code page}: node, subgraph, and edge lines. */
    private static Diagram diagram(String page) throws IOException {
        Path file = repositoryRoot().resolve(page);
        assertTrue(Files.isRegularFile(file), "Missing " + file);
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        int start = lines.indexOf("```mermaid");
        assertTrue(start >= 0, page + " has a mermaid block");
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
                assertTrue(line.startsWith("flowchart "), page + " is a Mermaid flowchart: " + line);
                header = true;
                continue;
            }
            String parent = stack.isEmpty() ? null : stack.get(stack.size() - 1);
            Matcher subgraph = SUBGRAPH.matcher(line);
            Matcher node = NODE.matcher(line);
            Matcher edge = EDGE.matcher(line);
            if (subgraph.matches()) {
                define(nodes, new Node(subgraph.group(1), subgraph.group(2), parent), page);
                stack.add(subgraph.group(1));
            } else if (line.equals("end")) {
                assertFalse(stack.isEmpty(), page + " line " + (index + 1) + ": end without subgraph");
                stack.remove(stack.size() - 1);
            } else if (edge.matches()) {
                edges.add(new Edge(edge.group(1), edge.group(3), edge.group(2) == null ? "" : edge.group(2), index + 1));
            } else if (node.matches()) {
                define(nodes, new Node(node.group(1), node.group(2), parent), page);
            } else {
                throw new AssertionError(page + " line " + (index + 1) + " is not a node, subgraph, or edge: " + line);
            }
        }
        assertTrue(stack.isEmpty(), page + " closes every subgraph");
        for (Edge edge : edges) {
            assertTrue(nodes.containsKey(edge.from()) && nodes.containsKey(edge.to()),
                    "line " + edge.line() + " connects an undefined node: " + edge);
        }
        return new Diagram(nodes, edges);
    }

    private static void define(Map<String, Node> nodes, Node node, String page) {
        assertFalse(nodes.containsKey(node.id()), page + " defines " + node.id() + " twice");
        nodes.put(node.id(), node);
    }

    /** A label without Mermaid's HTML line breaks and markup, with Mermaid's {@code #lt;} and {@code #gt;} decoded. */
    private static String plain(String label) {
        return label.replaceAll("<br\\s*/?>", " ").replaceAll("<[^>]+>", "").replace("&nbsp;", " ")
                .replace("#lt;", "<").replace("#gt;", ">");
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
