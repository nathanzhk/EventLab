import { mkdirSync, createWriteStream } from "node:fs";
import { dirname } from "node:path";

const logPath = "logs/tracing.log";
mkdirSync(dirname(logPath), { recursive: true });
const stream = createWriteStream(logPath, { flags: "a" });

function write(level: string, message: string): void {
  const line = `${new Date().toISOString()} ${level} ${message}`;
  stream.write(line + "\n");
  if (level === "ERROR") {
    console.error(line);
  } else {
    console.log(line);
  }
}

export function info(message: string): void {
  write("INFO", message);
}

export function error(message: string): void {
  write("ERROR", message);
}

export function closeLogger(): Promise<void> {
  return new Promise((resolve) => stream.end(resolve));
}
