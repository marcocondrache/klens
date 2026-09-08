import {
  FileJsonIcon,
  HardDriveIcon,
  LayersIcon,
  ShieldCheckIcon,
  UsersRoundIcon,
  type LucideIcon,
} from "lucide-react";

export interface Section {
  segment: string;
  label: string;
  icon: LucideIcon;
}

export const SECTIONS: Section[] = [
  { segment: "topics", label: "Topics", icon: LayersIcon },
  { segment: "groups", label: "Consumer Groups", icon: UsersRoundIcon },
  { segment: "schemas", label: "Schema Registry", icon: FileJsonIcon },
  { segment: "nodes", label: "Brokers", icon: HardDriveIcon },
  { segment: "acls", label: "ACLs", icon: ShieldCheckIcon },
];

export function findSection(segment: string | undefined) {
  return SECTIONS.find((section) => section.segment === segment);
}
