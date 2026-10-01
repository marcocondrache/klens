import { getRouteApi, useRouter } from "@tanstack/react-router";

import type { RecordAddress } from "@/lib/api/live";

const route = getRouteApi("/cluster/$cluster/topics_/$topic");

export function useRecordAddress() {
  const router = useRouter();
  const navigate = route.useNavigate();
  const params = route.useParams();
  const { partition, offset } = route.useSearch();
  const address: RecordAddress | null =
    partition == null || offset == null ? null : { partition, offset };

  function open(next: RecordAddress | null) {
    void navigate({
      search: (previous) => ({ ...previous, partition: next?.partition, offset: next?.offset }),
      replace: true,
    });
  }

  function link(target: RecordAddress) {
    const { href } = router.buildLocation({
      to: "/cluster/$cluster/topics/$topic",
      params,
      search: target,
    });
    return new URL(href, window.location.origin).href;
  }

  return { address, open, link };
}
