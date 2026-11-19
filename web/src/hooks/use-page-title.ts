import { useEffect } from "react";
import { useParams } from "@tanstack/react-router";

import { useClusterName } from "@/lib/clusters";
import { useActiveSection } from "@/lib/sections";

const APP_NAME = "klens";

/** Names the browser tab after the current view, most specific part first. */
export function usePageTitle() {
  const cluster = useClusterName();
  const section = useActiveSection();
  const detail = useParams({
    strict: false,
    shouldThrow: false,
    select: (params) => params?.topic ?? params?.group ?? params?.id,
  });

  const title = [detail, section?.label, cluster, APP_NAME].filter(Boolean).join(" · ");

  useEffect(() => {
    document.title = title;
  }, [title]);
}
