import { createColumnHelper } from "@tanstack/react-table";
import { KeyRoundIcon, Trash2Icon } from "lucide-react";

import type { DataTableFeatures } from "@/components/data-table/features";
import { IconButton } from "@/components/icon-button";
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

export function userActionColumn({
  onSetPassword,
  onDelete,
}: {
  onSetPassword?: (user: ScramUser) => void;
  onDelete?: (user: ScramUser) => void;
}) {
  return columnHelper.display({
    id: "action",
    enableResizing: false,
    meta: { align: "right", width: "5rem" },
    cell: ({ row }) => (
      <>
        {onSetPassword ? (
          <IconButton
            label={`Set a password for ${row.original.name}`}
            tooltip="Set password"
            reveal
            onClick={(event) => {
              event.stopPropagation();
              onSetPassword(row.original);
            }}
          >
            <KeyRoundIcon />
          </IconButton>
        ) : null}
        {onDelete ? (
          <IconButton
            label={`Delete a credential of ${row.original.name}`}
            tooltip="Delete"
            reveal
            onClick={(event) => {
              event.stopPropagation();
              onDelete(row.original);
            }}
          >
            <Trash2Icon />
          </IconButton>
        ) : null}
      </>
    ),
  });
}
