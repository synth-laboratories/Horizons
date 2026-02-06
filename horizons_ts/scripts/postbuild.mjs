import { mkdir, writeFile } from "node:fs/promises";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

// Ensure Node treats the CJS build as CommonJS even though the package root is ESM.
// This avoids needing .cjs extensions everywhere.
const cjsDir = path.resolve("dist/cjs");
await mkdir(cjsDir, { recursive: true });
await writeFile(
  path.join(cjsDir, "package.json"),
  JSON.stringify({ type: "commonjs" }, null, 2) + "\n",
  "utf8",
);

// Node ESM requires file extensions for relative imports (e.g. "./client.js").
// TypeScript emits extensionless imports by default; patch the built ESM output
// so the published package works in Node without a bundler.
const esmDir = path.resolve("dist/esm");

async function patchEsmImports(dir) {
  let entries;
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch {
    return;
  }

  await Promise.all(
    entries.map(async (ent) => {
      const p = path.join(dir, ent.name);
      if (ent.isDirectory()) return patchEsmImports(p);
      if (!ent.isFile() || !p.endsWith(".js")) return;

      const src = await readFile(p, "utf8");
      // Only touch relative specifiers that have no extension.
      const out = src.replace(
        /(from\s+["'])(\.\.?\/[^"'?#]+?)(["'])/g,
        (m, a, spec, b) => {
          // Leave already-suffixed imports alone.
          if (spec.endsWith(".js") || spec.endsWith(".json") || spec.endsWith(".node")) return m;
          return `${a}${spec}.js${b}`;
        },
      );

      if (out !== src) {
        await writeFile(p, out, "utf8");
      }
    }),
  );
}

await patchEsmImports(esmDir);
