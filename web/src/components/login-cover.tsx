export function LoginCover() {
  return (
    <div className="relative hidden overflow-hidden bg-background lg:block" aria-hidden>
      <div
        className="absolute inset-0"
        style={{
          backgroundImage: [
            "radial-gradient(ellipse 90% 70% at 75% 35%, color-mix(in oklab, var(--brand) 50%, transparent), transparent 68%)",
            "radial-gradient(ellipse 50% 45% at 10% 85%, color-mix(in oklab, var(--primary) 35%, transparent), transparent 60%)",
            "radial-gradient(ellipse 40% 35% at 90% 95%, color-mix(in oklab, var(--brand) 28%, transparent), transparent 55%)",
          ].join(", "),
        }}
      />
      <div
        className="absolute inset-0 bg-size-[3.5rem_3.5rem] mask-[radial-gradient(ellipse_at_center,black_20%,transparent_75%)]"
        style={{
          backgroundImage: [
            "linear-gradient(to right, color-mix(in oklab, var(--foreground) 4%, transparent) 1px, transparent 1px)",
            "linear-gradient(to bottom, color-mix(in oklab, var(--foreground) 4%, transparent) 1px, transparent 1px)",
          ].join(", "),
        }}
      />

      <svg
        className="absolute inset-0 size-full"
        viewBox="0 0 800 1000"
        preserveAspectRatio="xMidYMid slice"
        fill="none"
      >
        <path
          d="M-40 220C140 180 280 320 430 300C580 280 690 140 860 170"
          stroke="url(#klens-stream-1)"
          strokeWidth="1.25"
        />
        <path
          d="M-40 340C160 390 300 250 470 270C640 290 720 430 860 400"
          stroke="url(#klens-stream-2)"
          strokeWidth="1.25"
        />
        <path
          d="M-40 470C180 430 310 560 490 530C670 500 740 360 860 390"
          stroke="url(#klens-stream-1)"
          strokeWidth="1.25"
        />
        <path
          d="M-40 610C150 670 320 540 500 560C680 580 750 710 860 680"
          stroke="url(#klens-stream-2)"
          strokeWidth="1.25"
        />
        <circle cx="268" cy="308" r="3.5" fill="var(--brand-foreground)" fillOpacity="0.9" />
        <circle cx="268" cy="308" r="10" fill="var(--brand)" fillOpacity="0.28" />
        <circle cx="512" cy="278" r="2.5" fill="var(--primary)" fillOpacity="0.85" />
        <circle cx="188" cy="448" r="2.5" fill="var(--brand-foreground)" fillOpacity="0.7" />
        <circle cx="428" cy="538" r="3" fill="var(--primary)" fillOpacity="0.8" />
        <circle cx="428" cy="538" r="9" fill="var(--primary)" fillOpacity="0.18" />
        <defs>
          <linearGradient
            id="klens-stream-1"
            x1="0"
            y1="0"
            x2="800"
            y2="0"
            gradientUnits="userSpaceOnUse"
          >
            <stop offset="0" stopColor="var(--brand)" stopOpacity="0" />
            <stop offset="0.45" stopColor="var(--brand)" stopOpacity="0.7" />
            <stop offset="1" stopColor="var(--primary)" stopOpacity="0" />
          </linearGradient>
          <linearGradient
            id="klens-stream-2"
            x1="0"
            y1="0"
            x2="800"
            y2="0"
            gradientUnits="userSpaceOnUse"
          >
            <stop offset="0" stopColor="var(--primary)" stopOpacity="0" />
            <stop offset="0.5" stopColor="var(--brand)" stopOpacity="0.45" />
            <stop offset="1" stopColor="var(--brand)" stopOpacity="0" />
          </linearGradient>
        </defs>
      </svg>

      <img
        src="/favicon.svg"
        alt=""
        className="absolute top-[42%] left-1/2 w-[min(22rem,46%)] -translate-x-[42%] -translate-y-1/2 opacity-90"
        style={{
          filter: "drop-shadow(0 0 80px color-mix(in oklab, var(--brand) 55%, transparent))",
        }}
      />

      <div className="absolute inset-x-0 bottom-0 bg-linear-to-t from-background via-background/70 to-transparent p-10 pt-24">
        <p className="text-xl font-semibold tracking-tight text-foreground">
          Inspect a Kafka cluster
        </p>
        <p className="mt-1.5 max-w-md text-sm leading-relaxed text-muted-foreground">
          Browse topics, records, consumer groups, brokers, and schemas.
        </p>
      </div>
    </div>
  );
}
