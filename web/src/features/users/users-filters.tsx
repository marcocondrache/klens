import { KeyRoundIcon } from "lucide-react";

import type { FilterField } from "@/components/data-table/filters";
import type { ScramUser } from "@/lib/api/types";

import { SCRAM_MECHANISMS, type UserFilter } from "./search";
import { MECHANISM_LABEL } from "./users-columns";

export const USER_FILTERS: Array<FilterField<ScramUser, UserFilter>> = [
  {
    id: "mechanism",
    label: "Mechanism",
    plural: "mechanisms",
    icon: KeyRoundIcon,
    options: SCRAM_MECHANISMS.map((value) => ({ value, label: MECHANISM_LABEL[value] })),
    accessor: (user) => user.credentials.map((credential) => credential.mechanism),
  },
];

export function userMatches(user: ScramUser, needle: string) {
  return user.name.toLowerCase().includes(needle);
}
