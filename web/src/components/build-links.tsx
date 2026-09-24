import { cn } from "@/lib/utils";
import { RELEASE_URL, REPO_URL, VERSION } from "@/lib/build";

/** Version and repo links, set as a quiet footer line. */
export function BuildLinks({ className }: { className?: string }) {
  return (
    <div
      className={cn(
        "flex items-center justify-center gap-2 text-xs text-muted-foreground",
        className,
      )}
    >
      <a
        href={RELEASE_URL}
        target="_blank"
        rel="noreferrer"
        className="numeric hover:text-foreground"
      >
        v{VERSION}
      </a>
      <span aria-hidden className="text-muted-foreground/40">
        ·
      </span>
      <a href={REPO_URL} target="_blank" rel="noreferrer" className="hover:text-foreground">
        GitHub
      </a>
    </div>
  );
}
