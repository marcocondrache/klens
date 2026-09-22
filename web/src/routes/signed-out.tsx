import { CircleAlertIcon, LogOutIcon } from "lucide-react";
import { createFileRoute } from "@tanstack/react-router";

import { Button } from "@/components/ui/button";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { apiPath } from "@/lib/api/client";
import { parseSignedOutSearch } from "@/lib/route-search";

export const Route = createFileRoute("/signed-out")({
  validateSearch: parseSignedOutSearch,
  component: SignedOutPage,
});

function SignedOutPage() {
  const { error } = Route.useSearch();
  const { title, description } = copy(error);

  return (
    <Empty className="min-h-svh">
      <EmptyHeader>
        <EmptyMedia variant="icon">{error ? <CircleAlertIcon /> : <LogOutIcon />}</EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription>{description}</EmptyDescription>
      </EmptyHeader>
      <EmptyContent>
        <Button render={<a href={apiPath("/auth/login")} />}>
          {error ? "Try again" : "Sign in"}
        </Button>
      </EmptyContent>
    </Empty>
  );
}

function copy(error: string | undefined): { title: string; description: string } {
  if (error === "forbidden") {
    return {
      title: "Access denied",
      description: "Your account is not assigned a klens role.",
    };
  }
  if (error) {
    return {
      title: "Sign-in failed",
      description: "Try again, or check the identity provider.",
    };
  }
  return { title: "Signed out", description: "You have signed out of klens." };
}
