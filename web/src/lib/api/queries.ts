import { useQuery } from "@tanstack/react-query"

import * as api from "./client"
import type { RecordQuery } from "./types"

export const keys = {
  clusters: () => ["clusters"] as const,
  cluster: (cluster: string) => ["cluster", cluster] as const,
  throughput: (cluster: string) => ["cluster", cluster, "throughput"] as const,
  brokers: (cluster: string) => ["cluster", cluster, "brokers"] as const,
  broker: (cluster: string, id: number) => ["cluster", cluster, "brokers", id] as const,
  brokerConfigs: (cluster: string, id: number) => ["cluster", cluster, "brokers", id, "configs"] as const,
  topics: (cluster: string) => ["cluster", cluster, "topics"] as const,
  topic: (cluster: string, topic: string) => ["cluster", cluster, "topics", topic] as const,
  topicConfigs: (cluster: string, topic: string) => ["cluster", cluster, "topics", topic, "configs"] as const,
  topicThroughput: (cluster: string, topic: string) => ["cluster", cluster, "topics", topic, "throughput"] as const,
  records: (query: RecordQuery) => ["cluster", query.cluster, "topics", query.topic, "records", query] as const,
  groups: (cluster: string) => ["cluster", cluster, "groups"] as const,
  group: (cluster: string, group: string) => ["cluster", cluster, "groups", group] as const,
  subjects: (cluster: string) => ["cluster", cluster, "subjects"] as const,
  acls: (cluster: string) => ["cluster", cluster, "acls"] as const,
  search: (cluster: string, term: string) => ["cluster", cluster, "search", term] as const,
}

export function useClusters() {
  return useQuery({ queryKey: keys.clusters(), queryFn: api.listClusters })
}

export function useCluster(cluster: string) {
  return useQuery({ queryKey: keys.cluster(cluster), queryFn: () => api.getCluster(cluster) })
}

export function useClusterThroughput(cluster: string) {
  return useQuery({ queryKey: keys.throughput(cluster), queryFn: () => api.clusterThroughput(cluster) })
}

export function useBrokers(cluster: string) {
  return useQuery({ queryKey: keys.brokers(cluster), queryFn: () => api.listBrokers(cluster) })
}

export function useBroker(cluster: string, id: number) {
  return useQuery({
    queryKey: keys.broker(cluster, id),
    queryFn: () => api.getBroker(cluster, id),
    enabled: Number.isFinite(id),
  })
}

export function useBrokerConfigs(cluster: string, id: number) {
  return useQuery({
    queryKey: keys.brokerConfigs(cluster, id),
    queryFn: () => api.brokerConfigs(cluster, id),
    enabled: Number.isFinite(id),
  })
}

export function useTopics(cluster: string) {
  return useQuery({ queryKey: keys.topics(cluster), queryFn: () => api.listTopics(cluster) })
}

export function useTopic(cluster: string, topic: string) {
  return useQuery({ queryKey: keys.topic(cluster, topic), queryFn: () => api.getTopic(cluster, topic) })
}

export function useTopicConfigs(cluster: string, topic: string) {
  return useQuery({
    queryKey: keys.topicConfigs(cluster, topic),
    queryFn: () => api.topicConfigs(cluster, topic),
  })
}

export function useTopicThroughput(cluster: string, topic: string) {
  return useQuery({
    queryKey: keys.topicThroughput(cluster, topic),
    queryFn: () => api.topicThroughput(cluster, topic),
  })
}

export function useRecords(query: RecordQuery) {
  return useQuery({
    queryKey: keys.records(query),
    queryFn: () => api.fetchRecords(query),
    placeholderData: (previous) => previous,
  })
}

export function useConsumerGroups(cluster: string) {
  return useQuery({ queryKey: keys.groups(cluster), queryFn: () => api.listConsumerGroups(cluster) })
}

export function useConsumerGroup(cluster: string, group: string) {
  return useQuery({
    queryKey: keys.group(cluster, group),
    queryFn: () => api.getConsumerGroup(cluster, group),
  })
}

export function useSchemaSubjects(cluster: string) {
  return useQuery({ queryKey: keys.subjects(cluster), queryFn: () => api.listSchemaSubjects(cluster) })
}

export function useAcls(cluster: string) {
  return useQuery({ queryKey: keys.acls(cluster), queryFn: () => api.listAcls(cluster) })
}

export function useSearch(cluster: string, term: string) {
  return useQuery({
    queryKey: keys.search(cluster, term),
    queryFn: () => api.search(cluster, term),
    enabled: term.trim().length > 0,
  })
}
