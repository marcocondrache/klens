import { useMatchRoute, useNavigate } from "@tanstack/react-router";
import {
  FileJsonIcon,
  GaugeIcon,
  HardDriveIcon,
  KeyRoundIcon,
  LayersIcon,
  ShieldIcon,
  UsersRoundIcon,
  type LucideIcon,
} from "lucide-react";

import type { PrivilegeName } from "@/lib/api/types";

export type ClusterSection =
  | "topics"
  | "groups"
  | "schemas"
  | "nodes"
  | "acls"
  | "users"
  | "quotas";

export type SectionGroup = "cluster" | "insights";

export interface Section {
  segment: ClusterSection;
  label: string;
  icon: LucideIcon;
  group: SectionGroup;
  privilege?: PrivilegeName;
}

export const SECTION_GROUPS: Array<{ id: SectionGroup; label: string }> = [
  { id: "cluster", label: "Cluster" },
  { id: "insights", label: "Insights" },
];

export const SECTIONS: Section[] = [
  { segment: "topics", label: "Topics", icon: LayersIcon, group: "cluster" },
  { segment: "groups", label: "Consumer Groups", icon: UsersRoundIcon, group: "cluster" },
  { segment: "schemas", label: "Schema Registry", icon: FileJsonIcon, group: "cluster" },
  { segment: "nodes", label: "Brokers", icon: HardDriveIcon, group: "cluster" },
  { segment: "acls", label: "ACLs", icon: ShieldIcon, group: "cluster", privilege: "ACLS" },
  { segment: "users", label: "Users", icon: KeyRoundIcon, group: "cluster", privilege: "ACLS" },
  {
    segment: "quotas",
    label: "Quotas",
    icon: GaugeIcon,
    group: "insights",
    privilege: "BROKER_CONFIGS",
  },
];

export function visibleSections(can: (privilege: PrivilegeName) => boolean): Section[] {
  return SECTIONS.filter((section) => section.privilege == null || can(section.privilege));
}

export function clusterSectionTo(section: ClusterSection) {
  return `/cluster/$cluster/${section}` as const;
}

export function useActiveSection() {
  const matchRoute = useMatchRoute();
  return SECTIONS.find((section) =>
    matchRoute({ to: clusterSectionTo(section.segment), fuzzy: true }),
  );
}

/** Switches to another cluster, staying on the active section. */
export function useSwitchCluster() {
  const navigate = useNavigate();
  const section = useActiveSection();

  return (cluster: string) => {
    void navigate({
      to: section ? clusterSectionTo(section.segment) : "/cluster/$cluster",
      params: { cluster },
    });
  };
}
