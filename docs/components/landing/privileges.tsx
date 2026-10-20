const resources: { name: string; privileges: string[] }[] = [
  {
    name: 'Topics',
    privileges: ['create_topics', 'delete_topics', 'add_partitions', 'alter_topic_configs'],
  },
  { name: 'Records', privileges: ['produce', 'delete_records'] },
  { name: 'Consumer groups', privileges: ['reset_offsets', 'delete_offsets', 'delete_groups'] },
  { name: 'Schemas', privileges: ['register_schemas', 'set_compatibility', 'delete_schemas'] },
  { name: 'ACLs', privileges: ['create_acls', 'delete_acls'] },
  { name: 'SCRAM users', privileges: ['set_scram_credentials', 'delete_scram_credentials'] },
  { name: 'Client quotas', privileges: ['alter_quotas'] },
  { name: 'Brokers', privileges: ['alter_broker_configs'] },
];

export function WritePrivileges() {
  return (
    <dl className="@container overflow-hidden rounded-xl border bg-fd-popover text-sm text-fd-popover-foreground">
      {resources.map((resource) => (
        <div
          key={resource.name}
          className="grid gap-2 border-b border-fd-border/60 px-4 py-3 last:border-b-0 @md:grid-cols-[8.5rem_minmax(0,1fr)] @md:items-center @md:gap-4"
        >
          <dt className="font-medium">{resource.name}</dt>
          <dd className="flex flex-wrap gap-1.5">
            {resource.privileges.map((privilege) => (
              <span
                key={privilege}
                className="rounded-md border bg-fd-background px-1.5 py-0.5 font-mono text-[11px] text-fd-muted-foreground"
              >
                {privilege}
              </span>
            ))}
          </dd>
        </div>
      ))}
    </dl>
  );
}
