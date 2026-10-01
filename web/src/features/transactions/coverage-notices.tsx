import { EyeOffIcon, ShieldAlertIcon } from "lucide-react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import type { TransactionCoverage } from "@/lib/api/types";

export function CoverageNotices({ coverage }: { coverage: TransactionCoverage }) {
  const denied = coverage.deniedTopics;

  return (
    <>
      {denied.length > 0 ? (
        <Alert>
          <ShieldAlertIcon />
          <AlertTitle>
            {denied.length === 1
              ? "One topic went unchecked"
              : `${denied.length} topics went unchecked`}
          </AlertTitle>
          <AlertDescription>
            klens lacks Read on {denied.slice(0, 5).join(", ")}
            {denied.length > 5 ? ` and ${denied.length - 5} more` : ""}, so a hanging transaction
            there would not show.
          </AlertDescription>
        </Alert>
      ) : null}
      {coverage.unlistedProducers > 0 ? (
        <Alert>
          <EyeOffIcon />
          <AlertTitle>
            {coverage.unlistedProducers === 1
              ? "One producer's transactional id is hidden"
              : `${coverage.unlistedProducers} producers' transactional ids are hidden`}
          </AlertTitle>
          <AlertDescription>
            A transaction is open, but no coordinator lists its transactional id. klens likely lacks
            Describe on it, so it flags the partition only once the transaction passes the max
            timeout.
          </AlertDescription>
        </Alert>
      ) : null}
    </>
  );
}
