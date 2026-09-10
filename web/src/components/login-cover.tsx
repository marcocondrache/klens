export function LoginCover() {
  return (
    <div className="relative hidden overflow-hidden bg-[#0c0814] lg:block" aria-hidden>
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_90%_70%_at_75%_35%,oklch(0.48_0.26_306/0.55),transparent_68%),radial-gradient(ellipse_50%_45%_at_10%_85%,oklch(0.42_0.16_268/0.35),transparent_60%),radial-gradient(ellipse_40%_35%_at_90%_95%,oklch(0.55_0.14_210/0.28),transparent_55%)]" />
      <div className="absolute inset-0 bg-[linear-gradient(to_right,rgb(255_255_255/0.035)_1px,transparent_1px),linear-gradient(to_bottom,rgb(255_255_255/0.035)_1px,transparent_1px)] bg-size-[3.5rem_3.5rem] mask-[radial-gradient(ellipse_at_center,black_20%,transparent_75%)]" />

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
        <circle cx="268" cy="308" r="3.5" fill="#ede6ff" fillOpacity="0.9" />
        <circle cx="268" cy="308" r="10" fill="#863bff" fillOpacity="0.28" />
        <circle cx="512" cy="278" r="2.5" fill="#47bfff" fillOpacity="0.85" />
        <circle cx="188" cy="448" r="2.5" fill="#ede6ff" fillOpacity="0.7" />
        <circle cx="428" cy="538" r="3" fill="#47bfff" fillOpacity="0.8" />
        <circle cx="428" cy="538" r="9" fill="#47bfff" fillOpacity="0.18" />
        <defs>
          <linearGradient
            id="klens-stream-1"
            x1="0"
            y1="0"
            x2="800"
            y2="0"
            gradientUnits="userSpaceOnUse"
          >
            <stop offset="0" stopColor="#863bff" stopOpacity="0" />
            <stop offset="0.45" stopColor="#863bff" stopOpacity="0.7" />
            <stop offset="1" stopColor="#47bfff" stopOpacity="0" />
          </linearGradient>
          <linearGradient
            id="klens-stream-2"
            x1="0"
            y1="0"
            x2="800"
            y2="0"
            gradientUnits="userSpaceOnUse"
          >
            <stop offset="0" stopColor="#47bfff" stopOpacity="0" />
            <stop offset="0.5" stopColor="#863bff" stopOpacity="0.45" />
            <stop offset="1" stopColor="#863bff" stopOpacity="0" />
          </linearGradient>
        </defs>
      </svg>

      <img
        src="/favicon.svg"
        alt=""
        className="absolute top-[42%] left-1/2 w-[min(22rem,46%)] -translate-x-[42%] -translate-y-1/2 opacity-90 drop-shadow-[0_0_80px_oklch(0.5_0.25_306/0.55)]"
      />

      <div className="absolute inset-x-0 bottom-0 bg-linear-to-t from-[#0c0814] via-[#0c0814]/70 to-transparent p-10 pt-24">
        <p className="text-xl font-semibold tracking-tight text-white">A lens for Kafka</p>
        <p className="mt-1.5 max-w-md text-sm leading-relaxed text-white/55">
          Open a cluster and look through the topics and the messages on them, the groups consuming
          them, the brokers, and the schemas.
        </p>
      </div>
    </div>
  );
}
