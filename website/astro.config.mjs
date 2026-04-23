import { defineConfig } from "astro/config";
import mdx from "@astrojs/mdx";

export default defineConfig({
  site: "https://burla-cloud.github.io",
  base: "/dbt-burla",
  trailingSlash: "ignore",
  integrations: [mdx()],
  markdown: {
    shikiConfig: {
      themes: {
        light: "github-light",
        dark: "github-dark-dimmed",
      },
      wrap: true,
    },
  },
});
