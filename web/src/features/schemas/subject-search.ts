import type { SubjectRow } from "@/lib/api/types";

function schemaId(needle: string) {
  return /^\d+$/.test(needle) ? Number(needle) : null;
}

export function subjectMatches(subject: SubjectRow, needle: string) {
  const id = schemaId(needle);
  return (
    subject.subject.toLowerCase().includes(needle) ||
    (id !== null && subject.versions.some((entry) => entry.id === id))
  );
}

export function matchedVersion(subject: SubjectRow, needle: string) {
  const id = schemaId(needle);
  if (id === null || subject.id === id) return undefined;
  return subject.versions.findLast((entry) => entry.id === id)?.version;
}
