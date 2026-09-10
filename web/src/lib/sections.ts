import {
  FileJsonIcon,
  HardDriveIcon,
  LayersIcon,
  UsersRoundIcon,
  type LucideIcon,
} from "lucide-react";

export interface Section {
  segment: string;
  label: string;
  icon: LucideIcon;
  group: "browse" | "infra";
}

export const SECTIONS: Section[] = [
  { segment: "topics", label: "Topics", icon: LayersIcon, group: "browse" },
  { segment: "groups", label: "Consumer Groups", icon: UsersRoundIcon, group: "browse" },
  { segment: "schemas", label: "Schema Registry", icon: FileJsonIcon, group: "infra" },
  { segment: "nodes", label: "Brokers", icon: HardDriveIcon, group: "infra" },
];

export function findSection(segment: string | undefined) {
  return SECTIONS.find((section) => section.segment === segment);
}
