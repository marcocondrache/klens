import { LoginBackdrop } from "@/components/login-cover";
import { LoginForm } from "@/components/login-form";
import { ModeToggle } from "@/components/mode-toggle";

export function LoginPage() {
  return (
    <div className="dark relative flex min-h-svh flex-col text-white">
      <LoginBackdrop />

      <div className="relative z-10 flex items-center justify-between p-6 md:p-8">
        <a href="/" className="flex items-center gap-2 font-semibold tracking-tight text-white/90">
          <img src="/favicon.svg" alt="" className="size-6" />
          klens
        </a>
        <ModeToggle />
      </div>

      <div className="relative z-10 flex flex-1 flex-col items-center justify-center px-6 pb-16">
        <div className="motion-safe:animate-in motion-safe:fade-in motion-safe:slide-in-from-bottom-4 mb-10 flex flex-col items-center text-center duration-700">
          <div className="relative">
            <div className="absolute inset-0 -z-10 scale-[2.4] rounded-full bg-[radial-gradient(circle,oklch(0.6_0.18_48/0.35),transparent_70%)] blur-2xl" />
            <img
              src="/favicon.svg"
              alt=""
              className="size-18 drop-shadow-[0_0_60px_oklch(0.6_0.18_48/0.7)]"
            />
          </div>
          <p className="mt-6 text-xs font-medium tracking-[0.3em] text-[#FFB877] uppercase">
            Kafka, in focus
          </p>
          <h1 className="mt-3 bg-gradient-to-br from-[#FFE1A8] via-[#FF8B2E] to-[#E04F00] bg-clip-text text-5xl font-semibold tracking-tight text-transparent md:text-7xl">
            klens
          </h1>
          <p className="mt-4 max-w-md text-sm leading-relaxed text-white/55 md:text-base">
            Topics, messages, consumer groups, brokers, and schemas — one lens over your entire
            cluster.
          </p>
        </div>

        <div
          className="motion-safe:animate-in motion-safe:fade-in motion-safe:slide-in-from-bottom-6 relative w-full max-w-sm duration-700"
          style={{ animationDelay: "120ms" }}
        >
          <div className="absolute -inset-6 -z-10 rounded-[2.5rem] bg-[radial-gradient(closest-side,oklch(0.55_0.18_48/0.3),transparent)] blur-2xl" />

          <div className="relative rounded-3xl p-px shadow-2xl shadow-black/60">
            <div className="absolute inset-0 flex items-center justify-center overflow-hidden rounded-3xl">
              <div className="motion-safe:animate-spin-slow size-[250%] bg-[conic-gradient(from_0deg,transparent_0deg,#FBBF24_25deg,#FF8B2E_55deg,transparent_110deg,transparent_360deg)] opacity-80" />
            </div>
            <div className="relative rounded-3xl border border-white/[0.06] bg-[#150f0a]/90 p-8 backdrop-blur-2xl">
              <LoginForm />
            </div>
          </div>
        </div>
      </div>

      <p className="relative z-10 pb-6 text-center text-xs text-white/35">
        Sign-in is delegated to your organization's identity provider.
      </p>
    </div>
  );
}
