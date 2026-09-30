import { useMutation, useQueryClient, type QueryKey } from "@tanstack/react-query";
import { toast } from "sonner";

import { ApiError, apiErrorMessage } from "./client";

export type ApiMutation<TVariables, TData> = {
  mutationFn: (variables: TVariables) => Promise<TData>;
  /** Queries the change makes stale. They refetch before the mutation settles. */
  invalidates?: (variables: TVariables, data: TData) => readonly QueryKey[];
  success?: (variables: TVariables, data: TData) => string;
  failure: string;
};

/**
 * Refetches what `invalidates` names before settling. Catalog pages can lag
 * until the lanes the server kicked have polled, and the update stream then
 * refreshes them.
 */
export function useApiMutation<TVariables = void, TData = void>({
  mutationFn,
  invalidates,
  success,
  failure,
}: ApiMutation<TVariables, TData>) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn,
    onSuccess: async (data, variables) => {
      if (success) toast.success(success(variables, data));
      await Promise.all(
        (invalidates?.(variables, data) ?? []).map((queryKey) =>
          queryClient.invalidateQueries({ queryKey }),
        ),
      );
    },
    onError: (error) => {
      // The client is already on its way to the login page.
      if (error instanceof ApiError && error.status === 401) return;
      toast.error(failure, { description: apiErrorMessage(error, "Request failed") });
    },
  });
}
