import { CompassIcon } from "lucide-react";
import { Link } from "react-router";

import { Button } from "@/components/ui/button";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { formatModK } from "@/lib/keyboard";

export function NotFoundPage() {
  return (
    <Empty className="min-h-[60svh]">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <CompassIcon />
        </EmptyMedia>
        <EmptyTitle>Page not found</EmptyTitle>
        <EmptyDescription>
          That route does not exist in klens. Try the topics list or search with {formatModK()} or
          /.
        </EmptyDescription>
      </EmptyHeader>
      <EmptyContent>
        <Button render={<Link to="/" />}>Back to topics</Button>
      </EmptyContent>
    </Empty>
  );
}
