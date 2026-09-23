/** Quiet backdrop for the sign-in page: a faint grid and a soft brand glow behind the form. */
export function LoginCover() {
  return (
    <div className="pointer-events-none absolute inset-0 overflow-hidden" aria-hidden>
      <div
        className="absolute inset-0 bg-size-[3rem_3rem] mask-[radial-gradient(ellipse_60%_50%_at_50%_40%,black_10%,transparent_70%)]"
        style={{
          backgroundImage: [
            "linear-gradient(to right, color-mix(in oklab, var(--foreground) 5%, transparent) 1px, transparent 1px)",
            "linear-gradient(to bottom, color-mix(in oklab, var(--foreground) 5%, transparent) 1px, transparent 1px)",
          ].join(", "),
        }}
      />
      <div
        className="absolute top-[-12rem] left-1/2 h-[28rem] w-[44rem] -translate-x-1/2 rounded-full opacity-60 blur-3xl dark:opacity-40"
        style={{
          background:
            "radial-gradient(closest-side, color-mix(in oklab, var(--brand) 45%, transparent), transparent)",
        }}
      />
    </div>
  );
}
