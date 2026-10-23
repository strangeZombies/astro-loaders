import { defineConfig } from "astro/config";

// https://astro.build/config
export default defineConfig({
  output: "static",
  experimental: {
    liveContentCollections: true,
  },
  image: {
    domains: ["image.simplecastcdn.com"],
  },
});
