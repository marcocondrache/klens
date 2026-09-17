import { Area, AreaChart } from "recharts";
import { cn } from "@/lib/utils";

import { ChartContainer, type ChartConfig } from "@/components/ui/chart";
import type { Point } from "@/lib/api/types";

const SPARK_CONFIG = {
  value: { label: "Value", color: "var(--brand)" },
} satisfies ChartConfig;

export function Sparkline({ data, className }: { data: Point[]; className?: string }) {
  return (
    <ChartContainer config={SPARK_CONFIG} className={cn("h-9 w-full", className)}>
      <AreaChart data={data} margin={{ top: 2, right: 0, bottom: 0, left: 0 }}>
        <defs>
          <linearGradient id="spark-fill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--color-value)" stopOpacity={0.32} />
            <stop offset="100%" stopColor="var(--color-value)" stopOpacity={0} />
          </linearGradient>
        </defs>
        <Area
          dataKey="value"
          type="monotone"
          stroke="var(--color-value)"
          strokeWidth={1.5}
          fill="url(#spark-fill)"
          dot={false}
          activeDot={false}
        />
      </AreaChart>
    </ChartContainer>
  );
}
