import { highlightJsonHtml, tokenizeJson } from "./src/lib/json-highlight";

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message);
}

const source = JSON.stringify({
  note: "</script><script>window.__xss=1</script>",
  html: '"><img src=x onerror=alert(1)>',
  amp: "a&b",
});

const html = highlightJsonHtml(source);
assert(!html.includes("</script>"), "html must escape </script>");
assert(!html.includes("<script"), "html must not contain a script tag");
assert(!html.includes("<img"), "html must not contain an img tag");
assert(html.includes("&lt;/script&gt;"), "html must contain escaped script close");
assert(html.includes("&lt;script&gt;"), "html must contain escaped script open");
assert(html.includes("&lt;img"), "html must contain escaped img");
assert(html.includes("onerror=alert(1)"), "payload text stays visible after escape");
assert(html.includes("&amp;"), "html must escape ampersand");
assert(html.includes("&quot;"), "html must escape quotes");

const tokens = tokenizeJson(source);
const text = tokens.map((token) => token.value).join("");
assert(text.includes("</script><script>window.__xss=1</script>"), "tokens keep raw script text");
assert(text.includes("<img src=x onerror=alert(1)>"), "tokens keep raw img text");
assert(text.includes("a&b"), "tokens keep raw ampersand");
assert(
  tokens.some((token) => token.className === "string" && token.value.includes("</script>")),
  "the script payload is a string token, not HTML",
);

const keyHtml = highlightJsonHtml('{"</script>":"<img onerror=alert(1)>"}');
assert(!keyHtml.includes("</script>"), "keys must be escaped in html");
assert(!keyHtml.includes("<img"), "string values must not emit img tags");
assert(keyHtml.includes("&lt;/script&gt;"), "escaped key is present");
assert(keyHtml.includes("&lt;img"), "escaped value is present");

console.log("json-highlight escape tests passed");
