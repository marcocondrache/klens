import * as z from "zod/mini";

const brokerTabParam = z.catch(z._default(z.enum(["log-dirs", "config"]), "log-dirs"), "log-dirs");

export const brokerDetailSearch = z.object({
  tab: brokerTabParam,
});

export function brokerTab(value: unknown) {
  return z.parse(brokerTabParam, value);
}
