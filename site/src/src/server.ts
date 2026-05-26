import express from "express";
import path from "path";
export function createServer() {
  const app = express();
  app.use(express.static(path.join(__dirname, "../../public")));
  app.get("/health", (_, res) => res.json({ status: "ok" }));
  app.get("/api/health", (_, res) => res.json({ status: "ok" }));
  return app;
}
