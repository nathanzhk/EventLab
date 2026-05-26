import { readFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { WebSocketServer, type WebSocket } from "ws";

import { info } from "../logger.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dashboardHtmlPaths = [
  join(__dirname, "dashboard.html"),
  join(__dirname, "../../src/dashboard/dashboard.html"),
];

export interface DashboardBroadcaster {
  broadcast(data: string): void;
}

export class NoopDashboard implements DashboardBroadcaster {
  broadcast(): void {}
}

export class DashboardServer implements DashboardBroadcaster {
  private readonly clients = new Set<WebSocket>();
  private readonly server: Server;
  private readonly wss = new WebSocketServer({ noServer: true });

  constructor(
    private readonly host: string,
    private readonly port: number,
  ) {
    this.server = createServer(async (req, res) => {
      if (req.url === "/" || req.url === "/index.html") {
        const html = await readDashboardHtml();
        res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        res.end(html);
        return;
      }

      res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
      res.end("not found");
    });

    this.server.on("upgrade", (req, socket, head) => {
      if (req.url !== "/ws") {
        socket.destroy();
        return;
      }

      this.wss.handleUpgrade(req, socket, head, (ws) => {
        this.clients.add(ws);
        ws.on("close", () => this.clients.delete(ws));
        ws.on("error", () => this.clients.delete(ws));
      });
    });
  }

  listen(): Promise<void> {
    return new Promise((resolve) => {
      this.server.listen(this.port, this.host, () => {
        info(`dashboard listening on http://${this.host}:${this.port}`);
        resolve();
      });
    });
  }

  broadcast(data: string): void {
    for (const client of this.clients) {
      if (client.readyState === client.OPEN) {
        client.send(data);
      }
    }
  }

  close(): Promise<void> {
    for (const client of this.clients) {
      client.close();
    }
    this.wss.close();
    return new Promise((resolve, reject) => {
      this.server.close((err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }
}

async function readDashboardHtml(): Promise<string> {
  for (const path of dashboardHtmlPaths) {
    try {
      return await readFile(path, "utf8");
    } catch (err) {
      const code = err instanceof Error && "code" in err ? err.code : null;
      if (code !== "ENOENT") {
        throw err;
      }
    }
  }

  throw new Error("dashboard.html not found");
}
