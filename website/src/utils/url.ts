const BASE = import.meta.env.BASE_URL.replace(/\/$/, "");

export function withBase(path: string): string {
  if (path.startsWith("http://") || path.startsWith("https://")) return path;
  if (path.startsWith(BASE)) return path;
  if (!path.startsWith("/")) path = "/" + path;
  return BASE + path;
}
