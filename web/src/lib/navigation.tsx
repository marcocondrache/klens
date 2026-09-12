import { forwardRef, useEffect, useMemo, type ComponentProps } from "react";
import {
  Outlet,
  useLinkProps,
  useLocation,
  useNavigate as useRouterNavigate,
  useParams as useRouterParams,
  useSearch,
} from "@tanstack/react-router";

export { Outlet, useLocation };

type LinkProps = Omit<ComponentProps<"a">, "href"> & {
  to: string;
};

function asSearchString(value: unknown): string | null {
  if (typeof value === "string") return value === "" ? null : value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return null;
}

export const Link = forwardRef<HTMLAnchorElement, LinkProps>(function Link(
  { to, onClick, ...props },
  ref,
) {
  const linkProps = useLinkProps({ href: to, onClick } as never, ref as never);
  return <a {...props} {...linkProps} />;
});

export function Navigate({ to, replace }: { to: string; replace?: boolean }) {
  const navigate = useRouterNavigate();

  useEffect(() => {
    void navigate({ href: to, replace });
  }, [navigate, replace, to]);

  return null;
}

export function useNavigate() {
  const navigate = useRouterNavigate();

  return (to: string) => {
    void navigate({ href: to });
  };
}

export function useParams<T extends Record<string, string | undefined>>() {
  return useRouterParams({
    strict: false,
    shouldThrow: false,
  } as never) as T;
}

export function useSearchParams() {
  const search = useSearch({
    strict: false,
    shouldThrow: false,
  } as never) as Record<string, unknown>;
  const navigate = useRouterNavigate();

  const params = useMemo(() => {
    const next = new URLSearchParams();
    for (const [key, value] of Object.entries(search)) {
      const serialized = asSearchString(value);
      if (serialized === null) continue;
      next.set(key, serialized);
    }
    return next;
  }, [search]);

  function setParams(next: URLSearchParams, opts?: { replace?: boolean }) {
    const searchUpdate: Record<string, string> = {};
    next.forEach((value, key) => {
      searchUpdate[key] = value;
    });
    void navigate({
      to: ".",
      search: searchUpdate,
      replace: opts?.replace,
      resetScroll: false,
    });
  }

  return [params, setParams] as const;
}
