import { check } from "k6";
import {
  AdminClient,
  Consumer,
  KEY,
  Producer,
  SCHEMA_TYPE_JSON,
  SchemaRegistry,
  TOPIC_NAME_STRATEGY,
  VALUE,
} from "k6/x/kafka";

const BROKERS = [
  "b-1.loadtestmskzk.jp9s8r.c2.kafka.ap-south-1.amazonaws.com:9094",
  "b-2.loadtestmskzk.jp9s8r.c2.kafka.ap-south-1.amazonaws.com:9094",
];
const TOPIC = __ENV.TOPIC || "zk-test-json-topic";

const tlsConfig = {
  enableTls: true,
  insecureSkipTlsVerify: true,
};

const adminClient = new AdminClient({ brokers: BROKERS, tls: tlsConfig });
const producer = new Producer({ brokers: BROKERS, topic: TOPIC, tls: tlsConfig });
const consumer = new Consumer({
  brokers: BROKERS,
  topic: TOPIC,
  groupId: "k6-ssl-json-group",
  tls: tlsConfig,
});

// No URL = local serialization, no external schema registry needed
const schemaRegistry = new SchemaRegistry();

const keySchema = JSON.stringify({
  title: "EventKey",
  type: "object",
  properties: {
    correlationId: { type: "string" },
  },
  required: ["correlationId"],
});

const valueSchema = JSON.stringify({
  title: "EventValue",
  type: "object",
  properties: {
    event: { type: "string" },
    timestamp: { type: "number" },
    vu: { type: "number" },
    iter: { type: "number" },
  },
  required: ["event", "timestamp"],
});

const keySubject = schemaRegistry.getSubjectName({
  topic: TOPIC,
  element: KEY,
  subjectNameStrategy: TOPIC_NAME_STRATEGY,
  schema: keySchema,
});

const valueSubject = schemaRegistry.getSubjectName({
  topic: TOPIC,
  element: VALUE,
  subjectNameStrategy: TOPIC_NAME_STRATEGY,
  schema: valueSchema,
});

const keySchemaObject = schemaRegistry.createSchema({
  subject: keySubject,
  schema: keySchema,
  schemaType: SCHEMA_TYPE_JSON,
});

const valueSchemaObject = schemaRegistry.createSchema({
  subject: valueSubject,
  schema: valueSchema,
  schemaType: SCHEMA_TYPE_JSON,
});

export const options = {
  scenarios: {
    produce: {
      executor: "constant-vus",
      vus: 5,
      duration: "30s",
      exec: "produce",
    },
    consume: {
      executor: "constant-vus",
      vus: 1,
      duration: "35s",
      exec: "consume",
      startTime: "5s",
    },
  },
  thresholds: {
    kafka_writer_error_count: ["count == 0"],
    kafka_reader_error_count: ["count == 0"],
  },
};

export function setup() {
  const topics = adminClient.listTopics();
  const exists = topics.some((t) => (t.topic || t) === TOPIC);
  if (!exists) {
    adminClient.createTopic({ topic: TOPIC, numPartitions: 2, replicationFactor: 2 });
    console.log(`Created topic: ${TOPIC}`);
  }
}

export function produce() {
  producer.produce({
    messages: [
      {
        key: schemaRegistry.serialize({
          data: { correlationId: `vu-${__VU}-iter-${__ITER}` },
          schema: keySchemaObject,
          schemaType: SCHEMA_TYPE_JSON,
        }),
        value: schemaRegistry.serialize({
          data: {
            event: "load-test",
            timestamp: Date.now(),
            vu: __VU,
            iter: __ITER,
          },
          schema: valueSchemaObject,
          schemaType: SCHEMA_TYPE_JSON,
        }),
        headers: { "content-type": "application/json" },
      },
    ],
  });
}

export function consume() {
  const messages = consumer.consume({ maxMessages: 10 });

  check(messages, {
    "received at least one message": (msgs) => msgs.length > 0,
    "key deserializes correctly": (msgs) =>
      msgs.length > 0 &&
      schemaRegistry
        .deserialize({ data: msgs[0].key, schema: keySchemaObject, schemaType: SCHEMA_TYPE_JSON })
        .correlationId.startsWith("vu-"),
    "value deserializes correctly": (msgs) => {
      if (msgs.length === 0) return false;
      const val = schemaRegistry.deserialize({
        data: msgs[0].value,
        schema: valueSchemaObject,
        schemaType: SCHEMA_TYPE_JSON,
      });
      return val.event === "load-test" && val.timestamp > 0;
    },
  });
}

export function teardown() {
  producer.close();
  consumer.close();
  adminClient.close();
}
