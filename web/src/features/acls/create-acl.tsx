import { useId, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { PlusIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Field, FieldDescription, FieldError, FieldLabel, FieldTitle } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Sheet, SheetTrigger } from "@/components/ui/sheet";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { FieldCount } from "@/components/field-count";
import { FormSheetContent, SheetForm } from "@/components/write-form";
import { apiErrorMessage, clusterPathname, post } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type {
  AclOperation,
  AclPatternType,
  AclPermission,
  AclResourceType,
  CreateAcls,
} from "@/lib/api/types";
import { formatEnumLabel } from "@/lib/format";

const RESOURCES: { value: AclResourceType; label: string; operations: AclOperation[] }[] = [
  {
    value: "TOPIC",
    label: "Topic",
    operations: [
      "READ",
      "WRITE",
      "DESCRIBE",
      "CREATE",
      "DELETE",
      "ALTER",
      "DESCRIBE_CONFIGS",
      "ALTER_CONFIGS",
      "ALL",
    ],
  },
  { value: "GROUP", label: "Group", operations: ["READ", "DESCRIBE", "DELETE", "ALL"] },
  {
    value: "CLUSTER",
    label: "Cluster",
    operations: [
      "CREATE",
      "DESCRIBE",
      "ALTER",
      "CLUSTER_ACTION",
      "DESCRIBE_CONFIGS",
      "ALTER_CONFIGS",
      "IDEMPOTENT_WRITE",
      "ALL",
    ],
  },
  {
    value: "TRANSACTIONAL_ID",
    label: "Transactional ID",
    operations: ["WRITE", "DESCRIBE", "ALL"],
  },
  { value: "DELEGATION_TOKEN", label: "Delegation token", operations: ["DESCRIBE", "ALL"] },
];

// Kafka accepts no other name for the cluster resource.
const CLUSTER_NAME = "kafka-cluster";

const PRINCIPAL = /^[^:\s]+:\S/;

export function CreateAclSheet({ cluster }: { cluster: string }) {
  const [open, setOpen] = useState(false);

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger render={<Button variant="outline" className="ml-auto font-normal" />}>
        <PlusIcon data-icon="inline-start" className="text-muted-foreground" />
        Create ACL
      </SheetTrigger>
      <FormSheetContent>
        <CreateAclForm cluster={cluster} onCreated={() => setOpen(false)} />
      </FormSheetContent>
    </Sheet>
  );
}

function CreateAclForm({ cluster, onCreated }: { cluster: string; onCreated: () => void }) {
  const id = useId();
  const queryClient = useQueryClient();
  const [principal, setPrincipal] = useState("");
  const [host, setHost] = useState("*");
  const [permission, setPermission] = useState<AclPermission>("ALLOW");
  const [resourceType, setResourceType] = useState<AclResourceType>("TOPIC");
  const [name, setName] = useState("");
  const [pattern, setPattern] = useState<AclPatternType>("LITERAL");
  const [operations, setOperations] = useState<AclOperation[]>([]);

  const create = useMutation({
    mutationFn: (request: CreateAcls) => post(clusterPathname(cluster, "acls"), request),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: keys.acls(cluster), exact: true }),
  });

  const resource = RESOURCES.find((entry) => entry.value === resourceType) ?? RESOURCES[0];
  const onCluster = resourceType === "CLUSTER";
  const resourceName = onCluster ? CLUSTER_NAME : name.trim();
  const badPrincipal = principal !== "" && !PRINCIPAL.test(principal.trim());
  const ready =
    PRINCIPAL.test(principal.trim()) &&
    host.trim() !== "" &&
    resourceName !== "" &&
    operations.length > 0;

  function pickResource(next: AclResourceType) {
    const allowed = RESOURCES.find((entry) => entry.value === next)?.operations ?? [];
    setResourceType(next);
    setOperations((picked) => picked.filter((operation) => allowed.includes(operation)));
  }

  function submit() {
    create.mutate(
      {
        bindings: operations.map((operation) => ({
          resourceType,
          resourceName,
          patternType: onCluster ? "LITERAL" : pattern,
          principal: principal.trim(),
          host: host.trim(),
          operation,
          permission,
        })),
      },
      // Unlike a hook-level onSuccess, this one is dropped once the sheet closes.
      { onSuccess: onCreated },
    );
  }

  return (
    <SheetForm
      title="Create ACL"
      description="Each operation you pick becomes its own binding."
      error={create.isError ? apiErrorMessage(create.error, "Failed to create the ACL.") : null}
      submit={{
        label: operations.length > 1 ? `Create ${operations.length} bindings` : "Create ACL",
        pending: create.isPending,
        disabled: !ready,
      }}
      onSubmit={submit}
    >
      <Field data-invalid={badPrincipal || undefined}>
        <FieldLabel htmlFor={`${id}-principal`}>Principal</FieldLabel>
        <Input
          id={`${id}-principal`}
          data-autofocus
          autoComplete="off"
          spellCheck={false}
          placeholder="User:alice"
          className="font-mono"
          value={principal}
          aria-invalid={badPrincipal || undefined}
          aria-describedby={badPrincipal ? `${id}-principal-error` : undefined}
          onChange={(event) => setPrincipal(event.target.value)}
        />
        {badPrincipal ? (
          <FieldError id={`${id}-principal-error`}>
            Enter a principal such as User:alice.
          </FieldError>
        ) : null}
      </Field>

      <div className="grid grid-cols-2 items-start gap-3">
        <Field>
          <FieldLabel htmlFor={`${id}-host`}>Host</FieldLabel>
          <Input
            id={`${id}-host`}
            autoComplete="off"
            spellCheck={false}
            className="font-mono"
            value={host}
            onChange={(event) => setHost(event.target.value)}
          />
        </Field>
        <Field>
          <FieldTitle id={`${id}-permission`}>Permission</FieldTitle>
          <div>
            <ToggleGroup
              value={[permission]}
              onValueChange={(next) => {
                if (next[0]) setPermission(next[0] === "DENY" ? "DENY" : "ALLOW");
              }}
              variant="outline"
              size="sm"
              spacing={0}
              aria-labelledby={`${id}-permission`}
            >
              <ToggleGroupItem value="ALLOW">Allow</ToggleGroupItem>
              <ToggleGroupItem value="DENY">Deny</ToggleGroupItem>
            </ToggleGroup>
          </div>
        </Field>
      </div>

      <div className="grid grid-cols-2 items-start gap-3">
        <Field>
          <FieldLabel htmlFor={`${id}-resource`}>Resource</FieldLabel>
          <Select
            items={RESOURCES}
            value={resourceType}
            onValueChange={(next) => {
              if (next !== null) pickResource(next);
            }}
          >
            <SelectTrigger id={`${id}-resource`} className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {RESOURCES.map((entry) => (
                <SelectItem key={entry.value} value={entry.value}>
                  {entry.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>
        <Field data-disabled={onCluster || undefined}>
          <FieldTitle id={`${id}-pattern`}>Pattern</FieldTitle>
          <div>
            <ToggleGroup
              value={[onCluster ? "LITERAL" : pattern]}
              onValueChange={(next) => {
                if (next[0]) setPattern(next[0] === "PREFIXED" ? "PREFIXED" : "LITERAL");
              }}
              disabled={onCluster}
              variant="outline"
              size="sm"
              spacing={0}
              aria-labelledby={`${id}-pattern`}
            >
              <ToggleGroupItem value="LITERAL">Literal</ToggleGroupItem>
              <ToggleGroupItem value="PREFIXED">Prefixed</ToggleGroupItem>
            </ToggleGroup>
          </div>
        </Field>
      </div>

      <Field data-disabled={onCluster || undefined}>
        <FieldLabel htmlFor={`${id}-name`}>Name</FieldLabel>
        <Input
          id={`${id}-name`}
          autoComplete="off"
          spellCheck={false}
          className="font-mono"
          value={onCluster ? CLUSTER_NAME : name}
          disabled={onCluster}
          aria-describedby={`${id}-name-hint`}
          onChange={(event) => setName(event.target.value)}
        />
        <FieldDescription id={`${id}-name-hint`}>
          {onCluster
            ? "Cluster bindings cover the whole cluster."
            : pattern === "PREFIXED"
              ? `Matches every ${resource.label.toLowerCase()} whose name starts with this.`
              : `Use * to match every ${resource.label.toLowerCase()}.`}
        </FieldDescription>
      </Field>

      <Field>
        <FieldTitle id={`${id}-operations`}>
          Operations
          <FieldCount value={operations.length} />
        </FieldTitle>
        <div>
          <ToggleGroup
            multiple
            value={operations}
            onValueChange={(next) =>
              setOperations(resource.operations.filter((operation) => next.includes(operation)))
            }
            variant="outline"
            size="sm"
            className="flex-wrap"
            aria-labelledby={`${id}-operations`}
          >
            {resource.operations.map((operation) => (
              <ToggleGroupItem key={operation} value={operation}>
                {formatEnumLabel(operation)}
              </ToggleGroupItem>
            ))}
          </ToggleGroup>
        </div>
      </Field>
    </SheetForm>
  );
}
