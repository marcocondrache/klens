import { CompassIcon } from "lucide-react";
import { formatForDisplay } from "@tanstack/react-hotkeys";
import { Link } from "@tanstack/react-router";

import { buttonVariants } from "@/components/ui/button";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";

export function NotFoundPage() {
  return (
    <Empty className="min-h-[60svh]">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <CompassIcon />
        </EmptyMedia>
        <EmptyTitle>Page not found</EmptyTitle>
        <EmptyDescription>
          Check the address, or search with {formatForDisplay("Mod+K")} or /.
        </EmptyDescription>
      </EmptyHeader>
      <EmptyContent>
        <Link to="/" className={buttonVariants()}>
          Back to topics
        </Link>
      </EmptyContent>
    </Empty>
  );
}
