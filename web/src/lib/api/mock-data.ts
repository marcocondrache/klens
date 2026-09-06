import type {
  Acl,
  Broker,
  CleanupPolicy,
  Cluster,
  ConfigEntry,
  ConsumerGroup,
  ConsumerGroupMember,
  ConsumerGroupState,
  GroupOffset,
  MemberAssignment,
  Partition,
  SchemaSubject,
  ThroughputPoint,
  Topic,
} from "./types"

export const GENERATED_AT = Date.now()

function hashString(value: string) {
  let hash = 2166136261
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return hash >>> 0
}

export class Rand {
  private state: number

  constructor(seed: string) {
    this.state = hashString(seed)
  }

  next() {
    this.state = (this.state + 0x6d2b79f5) >>> 0
    let value = this.state
    value = Math.imul(value ^ (value >>> 15), value | 1)
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61)
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296
  }

  int(min: number, max: number) {
    return min + Math.floor(this.next() * (max - min + 1))
  }

  pick<T>(values: readonly T[]) {
    return values[this.int(0, values.length - 1)]
  }

  chance(probability: number) {
    return this.next() < probability
  }
}

interface ClusterDef {
  name: string
  label: string
  brokers: number
  securityProtocol: Cluster["securityProtocol"]
  version: string
  status: Cluster["status"]
  region: string
  scale: number
  topics: number
  groups: number
}

const CLUSTER_DEFS: ClusterDef[] = [
  {
    name: "local",
    label: "Local",
    brokers: 1,
    securityProtocol: "PLAINTEXT",
    version: "4.1.0",
    status: "HEALTHY",
    region: "localhost",
    scale: 0.01,
    topics: 9,
    groups: 4,
  },
  {
    name: "staging",
    label: "Staging",
    brokers: 3,
    securityProtocol: "SASL_SSL",
    version: "3.9.1",
    status: "HEALTHY",
    region: "eu-west-1",
    scale: 0.18,
    topics: 19,
    groups: 9,
  },
  {
    name: "production",
    label: "Production",
    brokers: 6,
    securityProtocol: "SASL_SSL",
    version: "3.9.1",
    status: "DEGRADED",
    region: "eu-west-1",
    scale: 1,
    topics: 26,
    groups: 14,
  },
]

interface TopicDef {
  name: string
  partitions: number
  cleanupPolicy: CleanupPolicy
  retentionDays: number
  weight: number
}

const TOPIC_DEFS: TopicDef[] = [
  { name: "orders.created", partitions: 12, cleanupPolicy: "DELETE", retentionDays: 7, weight: 90 },
  { name: "orders.updated", partitions: 12, cleanupPolicy: "DELETE", retentionDays: 7, weight: 62 },
  { name: "orders.cancelled", partitions: 6, cleanupPolicy: "DELETE", retentionDays: 7, weight: 8 },
  { name: "payments.authorized", partitions: 12, cleanupPolicy: "DELETE", retentionDays: 30, weight: 74 },
  { name: "payments.captured", partitions: 12, cleanupPolicy: "DELETE", retentionDays: 30, weight: 68 },
  { name: "payments.failed", partitions: 6, cleanupPolicy: "DELETE", retentionDays: 30, weight: 11 },
  { name: "inventory.reserved", partitions: 8, cleanupPolicy: "DELETE", retentionDays: 3, weight: 55 },
  { name: "inventory.released", partitions: 8, cleanupPolicy: "DELETE", retentionDays: 3, weight: 24 },
  { name: "inventory.snapshot", partitions: 6, cleanupPolicy: "COMPACT", retentionDays: 0, weight: 14 },
  { name: "shipments.dispatched", partitions: 6, cleanupPolicy: "DELETE", retentionDays: 14, weight: 31 },
  { name: "shipments.delivered", partitions: 6, cleanupPolicy: "DELETE", retentionDays: 14, weight: 28 },
  { name: "users.registered", partitions: 3, cleanupPolicy: "DELETE", retentionDays: 90, weight: 6 },
  { name: "users.profile", partitions: 6, cleanupPolicy: "COMPACT", retentionDays: 0, weight: 18 },
  { name: "notifications.email", partitions: 6, cleanupPolicy: "DELETE", retentionDays: 2, weight: 42 },
  { name: "notifications.push", partitions: 6, cleanupPolicy: "DELETE", retentionDays: 2, weight: 47 },
  { name: "analytics.pageviews", partitions: 24, cleanupPolicy: "DELETE", retentionDays: 1, weight: 240 },
  { name: "analytics.clicks", partitions: 24, cleanupPolicy: "DELETE", retentionDays: 1, weight: 186 },
  { name: "search.queries", partitions: 12, cleanupPolicy: "DELETE", retentionDays: 3, weight: 96 },
  { name: "cart.events", partitions: 12, cleanupPolicy: "DELETE", retentionDays: 3, weight: 118 },
  { name: "pricing.updates", partitions: 6, cleanupPolicy: "COMPACT", retentionDays: 0, weight: 9 },
  { name: "fraud.signals", partitions: 6, cleanupPolicy: "DELETE", retentionDays: 30, weight: 16 },
  { name: "audit.log", partitions: 3, cleanupPolicy: "DELETE", retentionDays: 365, weight: 12 },
  { name: "cdc.public.customers", partitions: 6, cleanupPolicy: "COMPACT_DELETE", retentionDays: 7, weight: 22 },
  { name: "cdc.public.orders", partitions: 12, cleanupPolicy: "COMPACT_DELETE", retentionDays: 7, weight: 58 },
  { name: "dead-letter.orders", partitions: 3, cleanupPolicy: "DELETE", retentionDays: 30, weight: 2 },
  { name: "__consumer_offsets", partitions: 50, cleanupPolicy: "COMPACT", retentionDays: 0, weight: 34 },
]

interface GroupDef {
  id: string
  topics: string[]
  state: ConsumerGroupState
  members: number
  lagFactor: number
}

const GROUP_DEFS: GroupDef[] = [
  { id: "order-processor", topics: ["orders.created", "orders.updated"], state: "STABLE", members: 6, lagFactor: 0.02 },
  { id: "payment-service", topics: ["payments.authorized", "payments.captured"], state: "STABLE", members: 4, lagFactor: 0.04 },
  { id: "inventory-sync", topics: ["inventory.reserved", "inventory.released"], state: "STABLE", members: 3, lagFactor: 0.01 },
  { id: "shipping-worker", topics: ["shipments.dispatched"], state: "STABLE", members: 2, lagFactor: 0.03 },
  { id: "notification-dispatcher", topics: ["notifications.email", "notifications.push"], state: "STABLE", members: 4, lagFactor: 0.08 },
  { id: "analytics-etl", topics: ["analytics.pageviews", "analytics.clicks"], state: "STABLE", members: 8, lagFactor: 0.42 },
  { id: "fraud-detector", topics: ["orders.created", "fraud.signals"], state: "STABLE", members: 3, lagFactor: 0.02 },
  { id: "search-indexer", topics: ["search.queries", "pricing.updates"], state: "PREPARING_REBALANCE", members: 2, lagFactor: 0.31 },
  { id: "dwh-sink-connector", topics: ["cdc.public.orders", "cdc.public.customers"], state: "STABLE", members: 4, lagFactor: 0.12 },
  { id: "cart-abandonment", topics: ["cart.events"], state: "STABLE", members: 3, lagFactor: 0.06 },
  { id: "audit-archiver", topics: ["audit.log"], state: "STABLE", members: 1, lagFactor: 0.01 },
  { id: "legacy-batch-job", topics: ["orders.created"], state: "EMPTY", members: 0, lagFactor: 0.94 },
  { id: "user-projection", topics: ["users.registered", "users.profile"], state: "STABLE", members: 2, lagFactor: 0.02 },
  { id: "dlq-monitor", topics: ["dead-letter.orders"], state: "STABLE", members: 1, lagFactor: 0 },
]

function buildBrokers(def: ClusterDef): Broker[] {
  const rand = new Rand(`${def.name}:brokers`)
  const zones = ["a", "b", "c"]

  return Array.from({ length: def.brokers }, (_, index) => {
    const local = def.name === "local"

    return {
      id: index + 1,
      host: local ? "localhost" : `kafka-${index}.${def.name}.${def.region}.internal`,
      port: local ? 9092 : 9094,
      rack: local ? null : `${def.region}${zones[index % zones.length]}`,
      controller: index === 0,
      partitionCount: 0,
      leaderCount: 0,
      logDirSizeBytes: 0,
      bytesInPerSec: Math.round(rand.int(60, 140) * 1024 * 1024 * def.scale),
      bytesOutPerSec: Math.round(rand.int(120, 260) * 1024 * 1024 * def.scale),
    }
  })
}

function buildTopics(def: ClusterDef, brokers: Broker[]): Topic[] {
  const defs = TOPIC_DEFS.slice(0, def.topics)
  const replicationFactor = Math.min(def.brokers, def.name === "local" ? 1 : 3)

  return defs.map((topicDef) => {
    const rand = new Rand(`${def.name}:${topicDef.name}`)
    const internal = topicDef.name.startsWith("_")
    const partitionCount =
      def.name === "local" ? Math.max(1, Math.round(topicDef.partitions / 4)) : topicDef.partitions

    const partitions: Partition[] = Array.from({ length: partitionCount }, (_, id) => {
      const leader = brokers[(id + hashString(topicDef.name)) % brokers.length].id
      const replicas = Array.from(
        { length: replicationFactor },
        (_, offset) => brokers[(id + hashString(topicDef.name) + offset) % brokers.length].id,
      )

      const degraded = def.status === "DEGRADED" && rand.chance(0.04)
      const isr = degraded && replicas.length > 1 ? replicas.slice(0, replicas.length - 1) : replicas

      const total = Math.round(topicDef.weight * def.scale * rand.int(9_000, 26_000))
      const low = topicDef.retentionDays === 0 ? 0 : Math.round(total * rand.next() * 0.3)

      return {
        id,
        leader,
        replicas,
        isr,
        lowWatermark: low,
        highWatermark: low + total,
        sizeBytes: total * rand.int(280, 1_400),
      }
    })

    const messageCount = partitions.reduce((sum, part) => sum + (part.highWatermark - part.lowWatermark), 0)
    const sizeBytes = partitions.reduce((sum, part) => sum + part.sizeBytes, 0)
    const messagesPerSec = Math.round(topicDef.weight * def.scale * rand.int(4, 12))

    return {
      name: topicDef.name,
      internal,
      partitions,
      replicationFactor,
      messageCount,
      sizeBytes,
      cleanupPolicy: topicDef.cleanupPolicy,
      retentionMs: topicDef.retentionDays === 0 ? -1 : topicDef.retentionDays * 86_400_000,
      consumerGroups: GROUP_DEFS.filter((group) => group.topics.includes(topicDef.name)).map((group) => group.id),
      bytesInPerSec: messagesPerSec * rand.int(320, 1_100),
      messagesPerSec,
      underReplicated: partitions.some((part) => part.isr.length < part.replicas.length),
    }
  })
}

function buildGroups(def: ClusterDef, topics: Topic[]): ConsumerGroup[] {
  const byName = new Map(topics.map((topic) => [topic.name, topic]))

  return GROUP_DEFS.slice(0, def.groups)
    .filter((groupDef) => groupDef.topics.some((topic) => byName.has(topic)))
    .map((groupDef) => {
      const rand = new Rand(`${def.name}:group:${groupDef.id}`)
      const groupTopics = groupDef.topics.filter((topic) => byName.has(topic))
      const memberCount = def.name === "local" ? Math.min(groupDef.members, 1) : groupDef.members

      const assignable = groupTopics.flatMap((topicName) =>
        byName.get(topicName)!.partitions.map((partition) => ({ topic: topicName, partition: partition.id })),
      )

      const members: ConsumerGroupMember[] = Array.from({ length: memberCount }, (_, index) => {
        const suffix = rand.int(0x100000, 0xffffff).toString(16)
        const owned = assignable.filter((_, position) => position % memberCount === index)
        const assignments: MemberAssignment[] = groupTopics
          .map((topicName) => ({
            topic: topicName,
            partitions: owned.filter((item) => item.topic === topicName).map((item) => item.partition),
          }))
          .filter((assignment) => assignment.partitions.length > 0)

        return {
          id: `${groupDef.id}-${index}-${suffix}`,
          clientId: `${groupDef.id}-${index}`,
          host: def.name === "local" ? "/127.0.0.1" : `/10.42.${rand.int(1, 24)}.${rand.int(2, 250)}`,
          assignments,
        }
      })

      const offsets: GroupOffset[] = groupTopics.flatMap((topicName) => {
        const topic = byName.get(topicName)!

        return topic.partitions.map((partition) => {
          const owner =
            members.find((member) =>
              member.assignments.some(
                (assignment) => assignment.topic === topicName && assignment.partitions.includes(partition.id),
              ),
            ) ?? null

          const available = partition.highWatermark - partition.lowWatermark
          const lag = Math.round(available * groupDef.lagFactor * rand.next())

          return {
            topic: topicName,
            partition: partition.id,
            currentOffset: partition.highWatermark - lag,
            endOffset: partition.highWatermark,
            lag,
            memberId: owner?.id ?? null,
          }
        })
      })

      return {
        id: groupDef.id,
        state: groupDef.state,
        protocol: groupDef.state === "EMPTY" ? "" : "cooperative-sticky",
        coordinator: rand.int(1, def.brokers),
        members,
        topics: groupTopics,
        lag: offsets.reduce((sum, offset) => sum + offset.lag, 0),
        offsets,
      }
    })
}

function buildCluster(def: ClusterDef): ClusterSnapshot {
  const brokers = buildBrokers(def)
  const topics = buildTopics(def, brokers)
  const groups = buildGroups(def, topics)

  for (const broker of brokers) {
    for (const topic of topics) {
      for (const partition of topic.partitions) {
        if (partition.replicas.includes(broker.id)) {
          broker.partitionCount += 1
          broker.logDirSizeBytes += Math.round(partition.sizeBytes / partition.replicas.length)
        }
        if (partition.leader === broker.id) {
          broker.leaderCount += 1
        }
      }
    }
  }

  const partitionCount = topics.reduce((sum, topic) => sum + topic.partitions.length, 0)

  const cluster: Cluster = {
    name: def.name,
    label: def.label,
    clusterId: `${def.name}-Xk3PmR${hashString(def.name).toString(36).slice(0, 6)}`,
    bootstrapServers: brokers.map((broker) => `${broker.host}:${broker.port}`).slice(0, 3),
    securityProtocol: def.securityProtocol,
    version: def.version,
    status: def.status,
    brokerCount: brokers.length,
    topicCount: topics.length,
    partitionCount,
    consumerGroupCount: groups.length,
    underReplicatedPartitions: topics.reduce(
      (sum, topic) => sum + topic.partitions.filter((part) => part.isr.length < part.replicas.length).length,
      0,
    ),
    offlinePartitions: 0,
    messageCount: topics.reduce((sum, topic) => sum + topic.messageCount, 0),
    sizeBytes: topics.reduce((sum, topic) => sum + topic.sizeBytes, 0),
    bytesInPerSec: brokers.reduce((sum, broker) => sum + broker.bytesInPerSec, 0),
    bytesOutPerSec: brokers.reduce((sum, broker) => sum + broker.bytesOutPerSec, 0),
  }

  return { cluster, brokers, topics, groups }
}

export interface ClusterSnapshot {
  cluster: Cluster
  brokers: Broker[]
  topics: Topic[]
  groups: ConsumerGroup[]
}

export const SNAPSHOTS = new Map<string, ClusterSnapshot>(
  CLUSTER_DEFS.map((def) => [def.name, buildCluster(def)]),
)

export const CLUSTERS = CLUSTER_DEFS.map((def) => SNAPSHOTS.get(def.name)!.cluster)

export function snapshot(name: string) {
  const found = SNAPSHOTS.get(name)
  if (!found) {
    throw new Error(`unknown cluster '${name}'`)
  }
  return found
}

export function throughputSeries(seed: string, base: number, points = 48): ThroughputPoint[] {
  const rand = new Rand(`${seed}:throughput`)
  const step = 60_000

  return Array.from({ length: points }, (_, index) => {
    const wave = Math.sin((index / points) * Math.PI * 2.4) * 0.22 + Math.sin(index / 3) * 0.06
    const factor = 1 + wave + (rand.next() - 0.5) * 0.16
    const bytesIn = Math.max(0, Math.round(base * factor))

    return {
      timestamp: GENERATED_AT - (points - 1 - index) * step,
      bytesIn,
      bytesOut: Math.round(bytesIn * (1.7 + rand.next() * 0.5)),
      messages: Math.round(bytesIn / 640),
    }
  })
}

const TOPIC_CONFIG_DOCS: Record<string, string> = {
  "cleanup.policy": "The retention policy to use on log segments.",
  "retention.ms": "How long a log is retained before discarding old segments.",
  "retention.bytes": "Maximum size a partition can grow to before discarding old segments.",
  "segment.bytes": "The segment file size for the log.",
  "min.insync.replicas": "Minimum replicas that must acknowledge a write for it to be considered successful.",
  "compression.type": "The final compression type for a given topic.",
  "max.message.bytes": "The largest record batch size allowed.",
}

export function topicConfigEntries(clusterName: string, topic: Topic): ConfigEntry[] {
  const rand = new Rand(`${clusterName}:${topic.name}:config`)

  const overrides: Record<string, string> = {
    "cleanup.policy": topic.cleanupPolicy === "COMPACT_DELETE" ? "compact,delete" : topic.cleanupPolicy.toLowerCase(),
    "retention.ms": String(topic.retentionMs),
    "min.insync.replicas": String(Math.max(1, topic.replicationFactor - 1)),
    "compression.type": rand.pick(["producer", "zstd", "lz4"]),
  }

  const defaults: Array<[string, string]> = [
    ["retention.bytes", "-1"],
    ["segment.bytes", "1073741824"],
    ["segment.ms", "604800000"],
    ["max.message.bytes", "1048588"],
    ["message.timestamp.type", "CreateTime"],
    ["min.cleanable.dirty.ratio", "0.5"],
    ["delete.retention.ms", "86400000"],
    ["flush.messages", "9223372036854775807"],
    ["index.interval.bytes", "4096"],
    ["preallocate", "false"],
    ["unclean.leader.election.enable", "false"],
    ["message.format.version", "3.0-IV1"],
  ]

  return [
    ...Object.entries(overrides).map(([name, value]) => ({
      name,
      value,
      source: "DYNAMIC_TOPIC_CONFIG" as const,
      readOnly: false,
      sensitive: false,
      documentation: TOPIC_CONFIG_DOCS[name] ?? null,
    })),
    ...defaults.map(([name, value]) => ({
      name,
      value,
      source: "DEFAULT_CONFIG" as const,
      readOnly: false,
      sensitive: false,
      documentation: TOPIC_CONFIG_DOCS[name] ?? null,
    })),
  ].sort((left, right) => left.name.localeCompare(right.name))
}

export function brokerConfigEntries(clusterName: string, broker: Broker): ConfigEntry[] {
  const rand = new Rand(`${clusterName}:broker:${broker.id}:config`)

  const entries: Array<[string, string, ConfigEntry["source"], boolean]> = [
    ["broker.id", String(broker.id), "STATIC_BROKER_CONFIG", true],
    ["broker.rack", broker.rack ?? "", "STATIC_BROKER_CONFIG", true],
    ["num.network.threads", String(rand.int(8, 16)), "DYNAMIC_BROKER_CONFIG", false],
    ["num.io.threads", String(rand.int(8, 24)), "DYNAMIC_BROKER_CONFIG", false],
    ["log.retention.hours", "168", "DEFAULT_CONFIG", false],
    ["log.segment.bytes", "1073741824", "DEFAULT_CONFIG", false],
    ["num.partitions", "6", "STATIC_BROKER_CONFIG", true],
    ["default.replication.factor", "3", "STATIC_BROKER_CONFIG", true],
    ["auto.create.topics.enable", "false", "STATIC_BROKER_CONFIG", true],
    ["message.max.bytes", "1048588", "DEFAULT_CONFIG", false],
    ["background.threads", "10", "DYNAMIC_BROKER_CONFIG", false],
    ["compression.type", "producer", "DEFAULT_CONFIG", false],
  ]

  const configs: ConfigEntry[] = entries.map(([name, value, source, readOnly]) => ({
    name,
    value,
    source,
    readOnly,
    sensitive: false,
    documentation: null,
  }))

  configs.push({
    name: "listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config",
    value: null,
    source: "STATIC_BROKER_CONFIG",
    readOnly: true,
    sensitive: true,
    documentation: null,
  })

  return configs.sort((left, right) => left.name.localeCompare(right.name))
}

export function schemaSubjects(clusterName: string, topics: Topic[]): SchemaSubject[] {
  return topics
    .filter((topic) => !topic.internal && !topic.name.startsWith("analytics"))
    .flatMap((topic) => ["key", "value"].map((suffix) => ({ topic, suffix })))
    .filter(({ suffix, topic }) => suffix === "value" || topic.name.startsWith("cdc."))
    .map(({ topic, suffix }, index) => {
      const rand = new Rand(`${clusterName}:schema:${topic.name}:${suffix}`)
      const latestVersion = rand.int(1, 7)
      const recordName = topic.name
        .split(/[.\-_]/)
        .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
        .join("")

      return {
        subject: `${topic.name}-${suffix}`,
        id: 1000 + index,
        type: rand.pick(["AVRO", "AVRO", "AVRO", "JSON", "PROTOBUF"] as const),
        latestVersion,
        versions: Array.from({ length: latestVersion }, (_, version) => version + 1),
        compatibility: rand.pick(["BACKWARD", "BACKWARD", "FULL", "NONE"] as const),
        schema: JSON.stringify(
          {
            type: "record",
            name: recordName,
            namespace: `com.klens.${topic.name.split(".")[0]}`,
            fields: [
              { name: "id", type: "string" },
              { name: "createdAt", type: { type: "long", logicalType: "timestamp-millis" } },
              { name: "payload", type: ["null", "string"], default: null },
              { name: "version", type: "int", default: latestVersion },
            ],
          },
          null,
          2,
        ),
      }
    })
}

export function acls(clusterName: string, topics: Topic[], groups: ConsumerGroup[]): Acl[] {
  const rand = new Rand(`${clusterName}:acls`)
  const principals = ["order-service", "payment-service", "analytics-etl", "connect-cluster", "admin"]

  const topicAcls: Acl[] = topics.slice(0, 12).flatMap((topic) => {
    const principal = rand.pick(principals)

    return (["Read", "Write", "Describe"] as const).slice(0, rand.int(1, 3)).map((operation) => ({
      principal: `User:${principal}`,
      resourceType: "TOPIC" as const,
      resourceName: topic.name,
      patternType: "LITERAL" as const,
      operation,
      permission: "ALLOW" as const,
      host: "*",
    }))
  })

  const groupAcls: Acl[] = groups.slice(0, 8).map((group) => ({
    principal: `User:${group.id}`,
    resourceType: "GROUP",
    resourceName: group.id,
    patternType: "LITERAL",
    operation: "Read",
    permission: "ALLOW",
    host: "*",
  }))

  return [
    ...topicAcls,
    ...groupAcls,
    {
      principal: "User:admin",
      resourceType: "CLUSTER",
      resourceName: "kafka-cluster",
      patternType: "LITERAL",
      operation: "All",
      permission: "ALLOW",
      host: "*",
    },
    {
      principal: "User:anonymous",
      resourceType: "TOPIC",
      resourceName: "payments.",
      patternType: "PREFIXED",
      operation: "All",
      permission: "DENY",
      host: "*",
    },
  ]
}
