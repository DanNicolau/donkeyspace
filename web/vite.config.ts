import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": process.env.DONKEYSPACE_API_PROXY_TARGET ?? "http://localhost:8080",
      "/healthz": process.env.DONKEYSPACE_API_PROXY_TARGET ?? "http://localhost:8080"
    }
  }
});
