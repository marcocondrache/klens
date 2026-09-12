/** Full-bleed animated backdrop for the login screen. Purely decorative. */
export function LoginBackdrop() {
  return (
    <div
      className="pointer-events-none fixed inset-0 -z-10 overflow-hidden bg-[#0d0906]"
      aria-hidden
    >
      <div
        className="motion-safe:animate-aurora-1 absolute top-[-25%] left-[-15%] size-[75%] rounded-full bg-[radial-gradient(circle,oklch(0.58_0.19_48/0.65),transparent_70%)] blur-3xl"
        style={{ willChange: "transform" }}
      />
      <div
        className="motion-safe:animate-aurora-2 absolute right-[-20%] bottom-[-30%] size-[80%] rounded-full bg-[radial-gradient(circle,oklch(0.62_0.16_60/0.5),transparent_70%)] blur-3xl"
        style={{ willChange: "transform" }}
      />
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_65%_55%_at_50%_0%,oklch(0.5_0.1_30/0.35),transparent_65%)]" />

      <div className="absolute inset-0 bg-[linear-gradient(to_right,rgb(255_255_255/0.035)_1px,transparent_1px),linear-gradient(to_bottom,rgb(255_255_255/0.035)_1px,transparent_1px)] bg-size-[3.5rem_3.5rem] mask-[radial-gradient(ellipse_75%_60%_at_50%_35%,black_15%,transparent_75%)]" />

      <svg
        className="absolute inset-0 size-full opacity-80"
        viewBox="0 0 1200 800"
        preserveAspectRatio="xMidYMid slice"
        fill="none"
      >
        <path
          d="M-60 180C220 120 380 300 620 260C860 220 1000 60 1260 110"
          stroke="url(#klens-stream-1)"
          strokeWidth="1.5"
          strokeDasharray="4 14"
          className="motion-safe:animate-stream-flow"
        />
        <path
          d="M-60 330C260 400 460 220 700 250C940 280 1040 460 1260 410"
          stroke="url(#klens-stream-2)"
          strokeWidth="1.5"
          strokeDasharray="3 16"
          className="motion-safe:animate-stream-flow"
          style={{ animationDelay: "-2.5s" }}
        />
        <path
          d="M-60 520C240 460 420 620 680 580C940 540 1060 360 1260 400"
          stroke="url(#klens-stream-1)"
          strokeWidth="1.5"
          strokeDasharray="4 14"
          className="motion-safe:animate-stream-flow"
          style={{ animationDelay: "-4.5s" }}
        />
        <path
          d="M-60 680C260 740 480 600 720 630C960 660 1080 760 1260 700"
          stroke="url(#klens-stream-2)"
          strokeWidth="1.5"
          strokeDasharray="3 16"
          className="motion-safe:animate-stream-flow"
          style={{ animationDelay: "-1s" }}
        />

        <circle
          cx="620"
          cy="262"
          r="4"
          fill="#FFF7ED"
          fillOpacity="0.9"
          className="motion-safe:animate-glow-pulse"
        />
        <circle cx="620" cy="262" r="12" fill="#FF8B2E" fillOpacity="0.22" />
        <circle
          cx="940"
          cy="278"
          r="3"
          fill="#FBBF24"
          fillOpacity="0.85"
          className="motion-safe:animate-glow-pulse"
          style={{ animationDelay: "-1.6s" }}
        />
        <circle
          cx="360"
          cy="440"
          r="3"
          fill="#FFF7ED"
          fillOpacity="0.7"
          className="motion-safe:animate-glow-pulse"
          style={{ animationDelay: "-2.4s" }}
        />
        <circle
          cx="680"
          cy="582"
          r="3.5"
          fill="#FBBF24"
          fillOpacity="0.8"
          className="motion-safe:animate-glow-pulse"
        />
        <circle cx="680" cy="582" r="10" fill="#FBBF24" fillOpacity="0.16" />

        <defs>
          <linearGradient
            id="klens-stream-1"
            x1="0"
            y1="0"
            x2="1200"
            y2="0"
            gradientUnits="userSpaceOnUse"
          >
            <stop offset="0" stopColor="#FF8B2E" stopOpacity="0" />
            <stop offset="0.45" stopColor="#FF8B2E" stopOpacity="0.75" />
            <stop offset="1" stopColor="#FBBF24" stopOpacity="0" />
          </linearGradient>
          <linearGradient
            id="klens-stream-2"
            x1="0"
            y1="0"
            x2="1200"
            y2="0"
            gradientUnits="userSpaceOnUse"
          >
            <stop offset="0" stopColor="#FBBF24" stopOpacity="0" />
            <stop offset="0.5" stopColor="#E04F00" stopOpacity="0.5" />
            <stop offset="1" stopColor="#E04F00" stopOpacity="0" />
          </linearGradient>
        </defs>
      </svg>

      <div className="absolute inset-0 bg-[radial-gradient(ellipse_70%_55%_at_50%_50%,transparent_40%,#0d0906_95%)]" />
    </div>
  );
}
