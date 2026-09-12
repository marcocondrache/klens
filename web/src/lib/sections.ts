import { useMatchRoute } from "@tanstack/react-router";
import {
  FileJsonIcon,
  HardDriveIcon,
  LayersIcon,
  UsersRoundIcon,
  type LucideIcon,
} from "lucide-react";

export type ClusterSection = "topics" | "groups" | "schemas" | "nodes";

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
];

export function clusterSectionTo(section: ClusterSection) {
  switch (section) {
    case "topics":
      return "/cluster/$cluster/topics" as const;
    case "groups":
      return "/cluster/$cluster/groups" as const;
    case "schemas":
      return "/cluster/$cluster/schemas" as const;
    case "nodes":
      return "/cluster/$cluster/nodes" as const;
  }
}

export function useActiveSection() {
  const matchRoute = useMatchRoute();
  return SECTIONS.find((section) =>
    matchRoute({ to: clusterSectionTo(section.segment), fuzzy: true }),
  );
}
