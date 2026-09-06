import {
  CLUSTERS,
  acls,
  brokerConfigEntries,
  schemaSubjects,
  snapshot,
  throughputSeries,
  topicConfigEntries,
} from "./mock-data"
import { buildRecord } from "./records"
import type {
  Acl,
  Broker,
  Cluster,
  ConfigEntry,
  ConsumerGroup,
  RecordQuery,
  SchemaSubject,
  SearchResult,
  ThroughputPoint,
  Topic,
  TopicRecord,
} from "./types"

/**
 * Stands in for the GraphQL API until the backend exposes topics, groups and
 * records. Every function mirrors the shape a resolver would return, so wiring
 * the real transport in means replacing the bodies here and nothing else.
 */

const MIN_LATENCY = 80
const MAX_LATENCY = 320

function resolve<T>(value: T): Promise<T> {
  const latency = MIN_LATENCY + Math.random() * (MAX_LATENCY - MIN_LATENCY)
  return new Promise((done) => setTimeout(() => done(value), latency))
}

function requireTopic(clusterName: string, topicName: string) {
  const topic = snapshot(clusterName).topics.find((candidate) => candidate.name === topicName)
  if (!topic) {
    throw new Error(`unknown topic '${topicName}' in cluster '${clusterName}'`)
  }
  return topic
}

export function listClusters(): Promise<Cluster[]> {
  return resolve(CLUSTERS)
}

export function getCluster(name: string): Promise<Cluster> {
  return resolve(snapshot(name).cluster)
}

export function listBrokers(clusterName: string): Promise<Broker[]> {
  return resolve(snapshot(clusterName).brokers)
}

export function getBroker(clusterName: string, id: number): Promise<Broker> {
  const broker = snapshot(clusterName).brokers.find((candidate) => candidate.id === id)
  if (!broker) {
    throw new Error(`unknown broker '${id}' in cluster '${clusterName}'`)
  }
  return resolve(broker)
}

export function brokerConfigs(clusterName: string, id: number): Promise<ConfigEntry[]> {
  const broker = snapshot(clusterName).brokers.find((candidate) => candidate.id === id)
  if (!broker) {
    throw new Error(`unknown broker '${id}' in cluster '${clusterName}'`)
  }
  return resolve(brokerConfigEntries(clusterName, broker))
}

export function listTopics(clusterName: string): Promise<Topic[]> {
  return resolve(snapshot(clusterName).topics)
}

export function getTopic(clusterName: string, topicName: string): Promise<Topic> {
  return resolve(requireTopic(clusterName, topicName))
}

export function topicConfigs(clusterName: string, topicName: string): Promise<ConfigEntry[]> {
  return resolve(topicConfigEntries(clusterName, requireTopic(clusterName, topicName)))
}

export function listConsumerGroups(clusterName: string): Promise<ConsumerGroup[]> {
  return resolve(snapshot(clusterName).groups)
}

export function getConsumerGroup(clusterName: string, groupId: string): Promise<ConsumerGroup> {
  const group = snapshot(clusterName).groups.find((candidate) => candidate.id === groupId)
  if (!group) {
    throw new Error(`unknown consumer group '${groupId}' in cluster '${clusterName}'`)
  }
  return resolve(group)
}

export function clusterThroughput(clusterName: string): Promise<ThroughputPoint[]> {
  const { cluster } = snapshot(clusterName)
  return resolve(throughputSeries(clusterName, cluster.bytesInPerSec))
}

export function topicThroughput(clusterName: string, topicName: string): Promise<ThroughputPoint[]> {
  const topic = requireTopic(clusterName, topicName)
  return resolve(throughputSeries(`${clusterName}:${topicName}`, topic.bytesInPerSec))
}

export function listSchemaSubjects(clusterName: string): Promise<SchemaSubject[]> {
  const { topics } = snapshot(clusterName)
  return resolve(schemaSubjects(clusterName, topics))
}

export function listAcls(clusterName: string): Promise<Acl[]> {
  const { topics, groups } = snapshot(clusterName)
  return resolve(acls(clusterName, topics, groups))
}

export function fetchRecords(query: RecordQuery): Promise<TopicRecord[]> {
  const topic = requireTopic(query.cluster, query.topic)
  const partitions =
    query.partition == null
      ? topic.partitions
      : topic.partitions.filter((partition) => partition.id === query.partition)

  const term = query.search.trim().toLowerCase()
  const window = Math.max(4, Math.ceil((query.limit * (term ? 8 : 2)) / Math.max(1, partitions.length)))

  const candidates = partitions.flatMap((partition) => {
    const available = partition.highWatermark - partition.lowWatermark
    const take = Math.min(window, available)

    return Array.from({ length: take }, (_, index) => {
      const offset =
        query.order === "newest"
          ? partition.highWatermark - 1 - index
          : partition.lowWatermark + index

      return buildRecord(query.cluster, topic, partition.id, offset)
    })
  })

  const matching = term
    ? candidates.filter(
        (record) =>
          record.key?.toLowerCase().includes(term) || record.value?.toLowerCase().includes(term),
      )
    : candidates

  const sorted = matching.sort((left, right) =>
    query.order === "newest" ? right.timestamp - left.timestamp : left.timestamp - right.timestamp,
  )

  return resolve(sorted.slice(0, query.limit))
}

export function search(clusterName: string, term: string): Promise<SearchResult[]> {
  const needle = term.trim().toLowerCase()
  if (!needle) {
    return resolve([])
  }

  const { topics, groups, brokers } = snapshot(clusterName)
  const base = `/cluster/${clusterName}`

  const results: SearchResult[] = [
    ...topics
      .filter((topic) => topic.name.toLowerCase().includes(needle))
      .map((topic) => ({
        kind: "topic" as const,
        id: topic.name,
        label: topic.name,
        detail: `${topic.partitions.length} partitions`,
        href: `${base}/topics/${encodeURIComponent(topic.name)}`,
      })),
    ...groups
      .filter((group) => group.id.toLowerCase().includes(needle))
      .map((group) => ({
        kind: "group" as const,
        id: group.id,
        label: group.id,
        detail: group.state,
        href: `${base}/groups/${encodeURIComponent(group.id)}`,
      })),
    ...brokers
      .filter((broker) => `${broker.id} ${broker.host}`.toLowerCase().includes(needle))
      .map((broker) => ({
        kind: "node" as const,
        id: String(broker.id),
        label: `Broker ${broker.id}`,
        detail: broker.host,
        href: `${base}/nodes/${broker.id}`,
      })),
  ]

  return resolve(results.slice(0, 20))
}
