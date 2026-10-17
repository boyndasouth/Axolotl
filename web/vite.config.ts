import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// `npm run dev` serves the console on :5173 and forwards API calls to the
// control plane on :8100. `npm run build` writes web/dist, which the control
// plane serves itself, so day to day you only need `python -m stream.server`.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: { "/api": { target: "http://127.0.0.1:8100", changeOrigin: true } },
  },
  build: { outDir: "dist", emptyOutDir: true, chunkSizeWarningLimit: 900 },
});
