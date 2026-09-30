import { useMatchRoute, useNavigate } from "@tanstack/react-router";
import {
  FileJsonIcon,
  HardDriveIcon,
  LayersIcon,
  ShieldIcon,
  UsersRoundIcon,
  type LucideIcon,
} from "lucide-react";

export type ClusterSection = "topics" | "groups" | "schemas" | "nodes" | "acls";

export interface Section {
  segment: ClusterSection;
  label: string;
  icon: LucideIcon;
}

export const SECTIONS: Section[] = [
  { segment: "topics", label: "Topics", icon: LayersIcon },
  { segment: "groups", label: "Consumer Groups", icon: UsersRoundIcon },
  { segment: "schemas", label: "Schema Registry", icon: FileJsonIcon },
  { segment: "nodes", label: "Brokers", icon: HardDriveIcon },
  { segment: "acls", label: "ACLs", icon: ShieldIcon },
];

export function visibleSections(canAcls: boolean): Section[] {
  return canAcls ? SECTIONS : SECTIONS.filter((section) => section.segment !== "acls");
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
