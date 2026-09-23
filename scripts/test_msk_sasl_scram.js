import { check } from "k6";
import { AdminClient, Consumer, Producer, SASL_SCRAM_SHA512 } from "k6/x/kafka";

const BROKERS = [
  "b-1.loadtestmskkraft.253v8w.c2.kafka.ap-south-1.amazonaws.com:9096",
  "b-1.loadtestmskkraft.253v8w.c2.kafka.ap-south-1.amazonaws.com:9096",
];
const TOPIC = __ENV.TOPIC || "msk-kraft-test-topic";

// SASL SCRAM-SHA-256 over TLS (SASL_SSL)
const saslConfig = {
  username: __ENV.KAFKA_USERNAME || "admin",
  password: __ENV.KAFKA_PASSWORD || "StrongP@ssw0rd!",
  algorithm: SASL_SCRAM_SHA512,
};

// TLS is required when using SASL on MSK
const tlsConfig = {
  enableTls: true,
  insecureSkipTlsVerify: true,
};

const adminClient = new AdminClient({ brokers: BROKERS, sasl: saslConfig, tls: tlsConfig });
const producer = new Producer({ brokers: BROKERS, topic: TOPIC, sasl: saslConfig, tls: tlsConfig });
const consumer = new Consumer({
  brokers: BROKERS,
  topic: TOPIC,
  groupId: "k6-scram-group",
  sasl: saslConfig,
  tls: tlsConfig,
});

export const options = {
  scenarios: {
    produce: {
      executor: "constant-vus",
      vus: 5,
      duration: "5s",
      exec: "produce",
    },
    consume: {
      executor: "constant-vus",
      vus: 1,
      duration: "10s",
      exec: "consume",
      startTime: "6s",
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
  } else {
    console.log(`Topic already exists: ${TOPIC}`);
  }
}

export function produce() {
  producer.produce({
    messages: [
      {
        key: `vu-${__VU}-iter-${__ITER}`,
        value: JSON.stringify({
          event: "test",
          timestamp: Date.now(),
          vu: __VU,
          iter: __ITER,
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
  });

  for (const msg of messages) {
    const value = JSON.parse(String.fromCharCode(...msg.value));
    console.log(
      `partition=${msg.partition} offset=${msg.offset} key=${String.fromCharCode(...msg.key)} vu=${value.vu}`
    );
  }
}

export function teardown() {
  producer.close();
  consumer.close();
  adminClient.close();
}
