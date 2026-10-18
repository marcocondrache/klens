import { EyeOffIcon } from 'lucide-react';
import { cn } from '@/lib/cn';

const clusters = ['production', 'staging'];

const roles: { group: string; privileges: (string[] | null)[] }[] = [
  {
    group: 'klens-admins',
    privileges: [
      ['records', 'topic_configs', '+21'],
      ['records', 'produce', '+21'],
    ],
  },
  { group: 'kafka-operators', privileges: [null, ['records', 'reset_offsets', '+2']] },
  {
    group: 'security-team',
    privileges: [
      ['acls', 'schema_text'],
      ['acls', 'schema_text'],
    ],
  },
  { group: 'klens-viewers', privileges: [[], []] },
];

export function AccessMatrix() {
  return (
    <div
      aria-hidden
      className="@container select-none overflow-hidden rounded-xl border bg-fd-popover text-xs text-fd-popover-foreground"
    >
      <div className="hidden grid-cols-[8.5rem_1fr_1fr] border-b text-fd-muted-foreground @md:grid">
        <span className="px-3 py-2.5">IdP group</span>
        {clusters.map((cluster) => (
          <span key={cluster} className="border-s px-3 py-2.5 font-mono">
            {cluster}
          </span>
        ))}
      </div>

      {roles.map((role) => (
        <div
          key={role.group}
          className="grid grid-cols-2 border-b border-fd-border/60 last:border-b-0 @md:grid-cols-[8.5rem_1fr_1fr]"
        >
          <span className="truncate px-3 pt-3 font-mono @max-md:col-span-2 @md:py-3">
            {role.group}
          </span>
          {role.privileges.map((privileges, i) => (
            <span
              key={clusters[i]}
              className={cn(
                'flex flex-wrap content-center items-center gap-1 border-s px-3 py-2',
                i === 0 && '@max-md:border-s-0',
              )}
            >
              <span className="w-full font-mono text-[10px] text-fd-muted-foreground @md:hidden">
                {clusters[i]}
              </span>
              {privileges === null ? (
                <span className="flex items-center gap-1.5 text-fd-muted-foreground">
                  <EyeOffIcon className="size-3.5" />
                  invisible
                </span>
              ) : privileges.length === 0 ? (
                <span className="text-fd-muted-foreground">catalog only</span>
              ) : (
                privileges.map((privilege) => (
                  <span
                    key={privilege}
                    className="rounded-md border border-brand/30 bg-brand/10 px-1.5 py-0.5 font-mono text-[11px]"
                  >
                    {privilege}
                  </span>
                ))
              )}
            </span>
          ))}
        </div>
      ))}
    </div>
  );
}
