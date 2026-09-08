import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from "recharts";
import { cn } from "@/lib/utils";

import { ChartContainer, ChartTooltip, type ChartConfig } from "@/components/ui/chart";
import { formatBytes, formatRate, formatTime } from "@/lib/format";
import type { ThroughputPoint } from "@/lib/api/types";

const THROUGHPUT_CONFIG = {
  bytesIn: { label: "Bytes in", color: "var(--chart-1)" },
  bytesOut: { label: "Bytes out", color: "var(--chart-2)" },
} satisfies ChartConfig;

interface TooltipPayload {
  active?: boolean;
  label?: string | number;
  payload?: Array<{
    dataKey?: string | number;
    name?: string | number;
    value?: number;
    color?: string;
  }>;
}

function ThroughputTooltip({ active, label, payload }: TooltipPayload) {
  if (!active || !payload?.length) return null;

  return (
    <div className="min-w-40 rounded-lg border bg-popover/95 p-2.5 text-xs shadow-md backdrop-blur">
      <p className="numeric mb-1.5 font-medium">{formatTime(label ?? 0)}</p>
      <div className="space-y-1">
        {payload.map((entry) => (
          <div key={String(entry.dataKey)} className="flex items-center justify-between gap-4">
            <span className="flex items-center gap-1.5 text-muted-foreground">
              <span className="size-2 rounded-full" style={{ backgroundColor: entry.color }} />
              {entry.dataKey === "bytesIn" ? "In" : "Out"}
            </span>
            <span className="numeric font-mono">{formatRate(Number(entry.value))}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function ThroughputChart({
  data,
  className,
}: {
  data: ThroughputPoint[];
  className?: string;
}) {
  return (
    <ChartContainer config={THROUGHPUT_CONFIG} className={cn("h-56 w-full", className)}>
      <AreaChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
        <defs>
          <linearGradient id="throughput-in" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--color-bytesIn)" stopOpacity={0.35} />
            <stop offset="100%" stopColor="var(--color-bytesIn)" stopOpacity={0.02} />
          </linearGradient>
          <linearGradient id="throughput-out" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--color-bytesOut)" stopOpacity={0.28} />
            <stop offset="100%" stopColor="var(--color-bytesOut)" stopOpacity={0.02} />
          </linearGradient>
        </defs>

        <CartesianGrid vertical={false} stroke="var(--border)" strokeDasharray="3 3" />
        <XAxis
          dataKey="timestamp"
          tickFormatter={(value) => formatTime(value).slice(0, 5)}
          tickLine={false}
          axisLine={false}
          minTickGap={40}
          tick={{ fontSize: 11, fill: "var(--muted-foreground)" }}
        />
        <YAxis
          tickFormatter={(value) => formatBytes(Number(value), 0)}
          tickLine={false}
          axisLine={false}
          width={58}
          tick={{ fontSize: 11, fill: "var(--muted-foreground)" }}
        />
        <ChartTooltip content={<ThroughputTooltip />} />

        <Area
          dataKey="bytesOut"
          type="monotone"
          stroke="var(--color-bytesOut)"
          strokeWidth={1.5}
          fill="url(#throughput-out)"
        />
        <Area
          dataKey="bytesIn"
          type="monotone"
          stroke="var(--color-bytesIn)"
          strokeWidth={1.75}
          fill="url(#throughput-in)"
        />
      </AreaChart>
    </ChartContainer>
  );
}

const SPARK_CONFIG = {
  messages: { label: "Messages", color: "var(--brand)" },
} satisfies ChartConfig;

export function Sparkline({ data, className }: { data: ThroughputPoint[]; className?: string }) {
  return (
    <ChartContainer config={SPARK_CONFIG} className={cn("h-9 w-full", className)}>
      <AreaChart data={data} margin={{ top: 2, right: 0, bottom: 0, left: 0 }}>
        <defs>
          <linearGradient id="spark-fill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--color-messages)" stopOpacity={0.32} />
            <stop offset="100%" stopColor="var(--color-messages)" stopOpacity={0} />
          </linearGradient>
        </defs>
        <Area
          dataKey="messages"
          type="monotone"
          stroke="var(--color-messages)"
          strokeWidth={1.5}
          fill="url(#spark-fill)"
          dot={false}
          activeDot={false}
        />
      </AreaChart>
    </ChartContainer>
  );
}
