import { createHighlighter } from "@tanstack/highlight/core";
import { json } from "@tanstack/highlight/languages/json";
import { createThemeCss } from "@tanstack/highlight/theme";
import { solarizedDarkTheme } from "@tanstack/highlight/themes/solarized-dark";
import { solarizedLightTheme } from "@tanstack/highlight/themes/solarized-light";

export const jsonHighlighter = createHighlighter({ languages: [json] });

const themeCss = createThemeCss({
  light: solarizedLightTheme,
  dark: solarizedDarkTheme,
  darkSelector: ".dark",
});

export function tokenizeJson(source: string) {
  return jsonHighlighter.tokenize(source, { lang: "json" }).tokens;
}

export function highlightJsonHtml(source: string) {
  return jsonHighlighter.highlightToHtml(source, { lang: "json" });
}

export function installJsonHighlightTheme() {
  if (typeof document === "undefined") return;
  if (document.getElementById("th-json-theme")) return;

  const style = document.createElement("style");
  style.id = "th-json-theme";
  style.textContent = themeCss;
  document.head.appendChild(style);
}
