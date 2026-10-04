import { useId, useState, type FormEvent } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon, PlusIcon } from "lucide-react";

import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Spinner } from "@/components/ui/spinner";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
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

export function CreateAclDialog({ cluster }: { cluster: string }) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={<Button />}>
        <PlusIcon data-icon="inline-start" />
        Create ACL
      </DialogTrigger>
      <DialogContent className="sm:max-w-lg">
        <CreateAclForm cluster={cluster} onCreated={() => setOpen(false)} />
      </DialogContent>
    </Dialog>
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
    operations.length > 0 &&
    !create.isPending;

  function pickResource(next: AclResourceType) {
    const allowed = RESOURCES.find((entry) => entry.value === next)?.operations ?? [];
    setResourceType(next);
    setOperations((picked) => picked.filter((operation) => allowed.includes(operation)));
  }

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!ready) return;
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
      // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
      { onSuccess: onCreated },
    );
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Create ACL</DialogTitle>
        <DialogDescription>Each operation you pick becomes its own binding.</DialogDescription>
      </DialogHeader>

      <div className="grid grid-cols-[minmax(0,1fr)_8rem] gap-3">
        <div className="grid gap-1.5">
          <Label htmlFor={`${id}-principal`}>Principal</Label>
          <Input
            id={`${id}-principal`}
            autoFocus
            autoComplete="off"
            spellCheck={false}
            placeholder="User:alice"
            className="font-mono"
            value={principal}
            aria-invalid={badPrincipal || undefined}
            onChange={(event) => setPrincipal(event.target.value)}
          />
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor={`${id}-host`}>Host</Label>
          <Input
            id={`${id}-host`}
            autoComplete="off"
            spellCheck={false}
            className="font-mono"
            value={host}
            onChange={(event) => setHost(event.target.value)}
          />
        </div>
      </div>

      <div className="grid gap-1.5">
        <Label>Permission</Label>
        <ToggleGroup
          value={[permission]}
          onValueChange={(next) => {
            if (next[0]) setPermission(next[0] === "DENY" ? "DENY" : "ALLOW");
          }}
          variant="outline"
          size="sm"
          spacing={0}
          aria-label="Permission"
        >
          <ToggleGroupItem value="ALLOW">Allow</ToggleGroupItem>
          <ToggleGroupItem value="DENY">Deny</ToggleGroupItem>
        </ToggleGroup>
      </div>

      <div className="grid grid-cols-[minmax(0,1fr)_auto] items-end gap-3">
        <div className="grid gap-1.5">
          <Label htmlFor={`${id}-resource`}>Resource</Label>
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
        </div>
        <ToggleGroup
          value={[onCluster ? "LITERAL" : pattern]}
          onValueChange={(next) => {
            if (next[0]) setPattern(next[0] === "PREFIXED" ? "PREFIXED" : "LITERAL");
          }}
          disabled={onCluster}
          variant="outline"
          size="sm"
          spacing={0}
          aria-label="Pattern"
        >
          <ToggleGroupItem value="LITERAL">Literal</ToggleGroupItem>
          <ToggleGroupItem value="PREFIXED">Prefixed</ToggleGroupItem>
        </ToggleGroup>
      </div>

      <div className="grid gap-1.5">
        <Label htmlFor={`${id}-name`}>Name</Label>
        <Input
          id={`${id}-name`}
          autoComplete="off"
          spellCheck={false}
          className="font-mono"
          value={onCluster ? CLUSTER_NAME : name}
          disabled={onCluster}
          onChange={(event) => setName(event.target.value)}
        />
        <p className="text-sm text-muted-foreground">
          {onCluster
            ? "Cluster bindings cover the whole cluster."
            : pattern === "PREFIXED"
              ? `Matches every ${resource.label.toLowerCase()} whose name starts with this.`
              : `Use * to match every ${resource.label.toLowerCase()}.`}
        </p>
      </div>

      <div className="grid gap-1.5">
        <Label>Operations</Label>
        <ToggleGroup
          multiple
          value={operations}
          onValueChange={(next) =>
            setOperations(resource.operations.filter((operation) => next.includes(operation)))
          }
          variant="outline"
          size="sm"
          className="flex-wrap"
          aria-label="Operations"
        >
          {resource.operations.map((operation) => (
            <ToggleGroupItem key={operation} value={operation}>
              {formatEnumLabel(operation)}
            </ToggleGroupItem>
          ))}
        </ToggleGroup>
      </div>

      {create.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription className="break-words">
            {apiErrorMessage(create.error, "Failed to create the ACL.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" disabled={!ready}>
          {create.isPending ? <Spinner data-icon="inline-start" /> : null}
          {operations.length > 1 ? `Create ${operations.length} bindings` : "Create ACL"}
        </Button>
      </DialogFooter>
    </form>
  );
}
