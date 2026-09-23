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
    </div>
  );
}
