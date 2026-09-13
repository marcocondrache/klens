import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/cluster/$cluster/")({
  beforeLoad: ({ params }) => {
    throw redirect({ to: "/cluster/$cluster/topics", params });
  },
});
