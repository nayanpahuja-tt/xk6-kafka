import { check } from "k6";
import {
  AdminClient,
  Consumer,
  Producer,
  RECORD_NAME_STRATEGY,
  SCHEMA_TYPE_PROTOBUF,
  SchemaRegistry,
  VALUE,
} from "k6/x/kafka";

const BROKERS = [
  "b-1.loadtestmskzk.jp9s8r.c2.kafka.ap-south-1.amazonaws.com:9094",
  "b-2.loadtestmskzk.jp9s8r.c2.kafka.ap-south-1.amazonaws.com:9094",
];
const TOPIC = __ENV.TOPIC || "zk-test-protobuf-topic";

const tlsConfig = {
  enableTls: true,
  insecureSkipTlsVerify: true,
};

const adminClient = new AdminClient({ brokers: BROKERS, tls: tlsConfig });
const producer = new Producer({ brokers: BROKERS, topic: TOPIC, tls: tlsConfig });
const consumer = new Consumer({
  brokers: BROKERS,
  topic: TOPIC,
  groupId: "k6-ssl-protobuf-group",
  tls: tlsConfig,
});

// No URL = standalone mode: inline .proto schema, no external schema registry needed
const schemaRegistry = new SchemaRegistry();

const protoSchema = `
syntax = "proto3";
package com.example.loadtest;

message LoadEvent {
  string correlation_id = 1;
  string event          = 2;
  int64  timestamp      = 3;
  int32  vu             = 4;
  int32  iter           = 5;
}
`;

const messageName = "com.example.loadtest.LoadEvent";

const valueSubject = schemaRegistry.getSubjectName({
  topic: TOPIC,
  element: VALUE,
  subjectNameStrategy: RECORD_NAME_STRATEGY,
  schema: protoSchema,
  messageName: messageName,
});

const valueSchemaObject = schemaRegistry.createSchema({
  subject: valueSubject,
  schema: protoSchema,
  schemaType: SCHEMA_TYPE_PROTOBUF,
  messageName: messageName,
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
        value: schemaRegistry.serialize({
          data: {
            correlationId: `vu-${__VU}-iter-${__ITER}`,
            event: "load-test",
            timestamp: Date.now(),
            vu: __VU,
            iter: __ITER,
          },
          schema: valueSchemaObject,
          schemaType: SCHEMA_TYPE_PROTOBUF,
        }),
        headers: { "content-type": "application/protobuf" },
      },
    ],
  });
}

export function consume() {
  const messages = consumer.consume({ maxMessages: 10 });

  check(messages, {
    "received at least one message": (msgs) => msgs.length > 0,
    "protobuf payload deserializes": (msgs) => {
      if (msgs.length === 0) return false;
      const val = schemaRegistry.deserialize({
        data: msgs[0].value,
        schema: valueSchemaObject,
        schemaType: SCHEMA_TYPE_PROTOBUF,
      });
      return val.event === "load-test" && val.timestamp > 0;
    },
    "correlation_id is present": (msgs) => {
      if (msgs.length === 0) return false;
      const val = schemaRegistry.deserialize({
        data: msgs[0].value,
        schema: valueSchemaObject,
        schemaType: SCHEMA_TYPE_PROTOBUF,
      });
      return val.correlationId.startsWith("vu-");
    },
  });
}

export function teardown() {
  producer.close();
  consumer.close();
  adminClient.close();
}
