import { useState } from "react";
import type { ColumnVisibilityState } from "@tanstack/react-table";

// Whether a column earns its space depends on the topic's data, so the choice is kept per topic.
function storageKey(cluster: string, topic: string) {
  return `klens:record-columns:${cluster}:${topic}`;
}

function isVisibilityState(value: unknown): value is ColumnVisibilityState {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    Object.values(value).every((visible) => typeof visible === "boolean")
  );
}

// Storage can be blocked or full. The choice then lasts only as long as the page.
function load(key: string): ColumnVisibilityState {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(key) ?? "{}");
    return isVisibilityState(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function save(key: string, visibility: ColumnVisibilityState) {
  try {
    localStorage.setItem(key, JSON.stringify(visibility));
  } catch {
    // The choice still applies to this page, see load.
  }
}

export function useRecordColumnVisibility(cluster: string, topic: string) {
  const key = storageKey(cluster, topic);
  const [stored, setStored] = useState(() => ({ key, visibility: load(key) }));

  // The view stays mounted when the route moves to another topic.
  const current = stored.key === key ? stored : { key, visibility: load(key) };
  if (current !== stored) setStored(current);

  function setVisibility(visibility: ColumnVisibilityState) {
    save(key, visibility);
    setStored({ key, visibility });
  }

  return [current.visibility, setVisibility] as const;
}
