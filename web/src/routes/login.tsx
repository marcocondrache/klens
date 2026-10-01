import { createFileRoute } from "@tanstack/react-router";

import { LoginPage } from "@/features/auth/login-page";
import { loginSearch } from "@/features/auth/search";

export const Route = createFileRoute("/login")({
  validateSearch: loginSearch,
  component: LoginPage,
});
