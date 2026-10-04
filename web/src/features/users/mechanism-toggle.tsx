import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import type { ScramMechanism } from "@/lib/api/types";

import { SCRAM_MECHANISMS } from "./search";
import { MECHANISM_LABEL } from "./users-columns";

export function MechanismToggle({
  value,
  options = SCRAM_MECHANISMS,
  onChange,
}: {
  value: ScramMechanism;
  options?: readonly ScramMechanism[];
  onChange: (mechanism: ScramMechanism) => void;
}) {
  return (
    <ToggleGroup
      value={[value]}
      onValueChange={(next) => {
        const picked = options.find((option) => option === next[0]);
        if (picked) onChange(picked);
      }}
      variant="outline"
      size="sm"
      spacing={0}
      aria-label="Mechanism"
    >
      {options.map((option) => (
        <ToggleGroupItem key={option} value={option}>
          {MECHANISM_LABEL[option]}
        </ToggleGroupItem>
      ))}
    </ToggleGroup>
  );
}
