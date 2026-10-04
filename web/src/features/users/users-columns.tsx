import type { ReactNode } from "react";
import { createColumnHelper } from "@tanstack/react-table";

import type { DataTableFeatures } from "@/components/data-table/features";
import type { ScramMechanism, ScramUser } from "@/lib/api/types";
import { formatNumber } from "@/lib/format";

export const MECHANISM_LABEL: Record<ScramMechanism, string> = {
  SHA256: "SCRAM-SHA-256",
  SHA512: "SCRAM-SHA-512",
};

function iterations(user: ScramUser, mechanism: ScramMechanism) {
  return user.credentials.find((credential) => credential.mechanism === mechanism)?.iterations;
}

const columnHelper = createColumnHelper<DataTableFeatures, ScramUser>();

function mechanismColumn(mechanism: ScramMechanism) {
  return columnHelper.accessor((user) => iterations(user, mechanism) ?? -1, {
    id: mechanism,
    header: MECHANISM_LABEL[mechanism],
    meta: { align: "right", width: "10rem" },
    cell: ({ row }) => {
      const count = iterations(row.original, mechanism);
      return count === undefined ? (
        <span className="text-muted-foreground/60">—</span>
      ) : (
        <span className="numeric">{formatNumber(count)} iterations</span>
      );
    },
  });
}

export const userColumns = columnHelper.columns([
  columnHelper.accessor("name", {
    header: "User",
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  mechanismColumn("SHA256"),
  mechanismColumn("SHA512"),
]);

export function userActionColumn(action: (user: ScramUser) => ReactNode) {
  return columnHelper.display({
    id: "action",
    enableResizing: false,
    meta: { align: "right", width: "5rem" },
    cell: ({ row }) => action(row.original),
  });
}
