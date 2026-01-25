// index.js (worker: processBatch only)
import express from "express";
import { Storage } from "@google-cloud/storage";
import vision from "@google-cloud/vision";
import { Firestore } from "@google-cloud/firestore";

import { supabase } from "./supabaseClient.js";
import { registerProcessBatchRoutes } from "./processBatch.js";

// ===== ENV =====
const GCS_BUCKET = process.env.GCS_BUCKET || "";
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID || undefined;

// ===== INIT =====
const app = express();
app.use(express.json());

const storage = new Storage();
const bucket = GCS_BUCKET ? storage.bucket(GCS_BUCKET) : null;
const visionClient = new vision.ImageAnnotatorClient();

const customerDb = new Firestore(
  FIREBASE_PROJECT_ID
    ? { projectId: FIREBASE_PROJECT_ID, databaseId: "akiyama-system" }
    : { databaseId: "akiyama-system" }
);

// ===== UTIL =====
function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`TIMEOUT ${label} after ${ms}ms`)), ms)),
  ]);
}
function parseGsUri(uri) {
  const m = uri?.match(/^gs:\/\/([^/]+)\/(.+)$/);
  return m ? { bucket: m[1], path: m[2] } : null;
}

// ===== OCR (FAX) =====
async function ocrFirstPageFromFile(gcsUri, mimeType) {
  const info = parseGsUri(gcsUri);
  if (!info) return { text: "" };

  const tmpPrefix = `_vision/${Date.now()}_${Math.random().toString(36).slice(2)}/`;
  const outUri = `gs://${info.bucket}/${tmpPrefix}`;

  const [op] = await visionClient.asyncBatchAnnotateFiles({
    requests: [
      {
        inputConfig: { gcsSource: { uri: gcsUri }, mimeType },
        features: [{ type: "DOCUMENT_TEXT_DETECTION" }],
        outputConfig: { gcsDestination: { uri: outUri }, batchSize: 1 },
        pages: [1],
      },
    ],
  });

  await withTimeout(op.promise(), 150000, "vision op.promise");

  const [files] = await storage.bucket(info.bucket).getFiles({ prefix: tmpPrefix });
  if (!files.length) return { text: "" };

  const [buf] = await files[0].download();
  await Promise.all(files.map((f) => f.delete().catch(() => {})));

  const json = JSON.parse(buf.toString());
  const text = json.responses?.[0]?.fullTextAnnotation?.text || "";
  return { text };
}

async function runOcrForAttachments(attachments) {
  let full = "";
  for (const uri of attachments || []) {
    if (!uri?.startsWith("gs://")) continue;
    const lower = uri.toLowerCase();

    try {
      let r;
      if (lower.endsWith(".pdf")) r = await ocrFirstPageFromFile(uri, "application/pdf");
      else if (lower.endsWith(".tif") || lower.endsWith(".tiff")) r = await ocrFirstPageFromFile(uri, "image/tiff");
      else {
        const [res] = await withTimeout(
          visionClient.documentTextDetection({ image: { source: { imageUri: uri } } }),
          60000,
          "vision documentTextDetection(image)"
        );
        const text = res.fullTextAnnotation?.text || res.textAnnotations?.[0]?.description || "";
        r = { text };
      }

      if (r?.text) full += (full ? "\n" : "") + r.text;
    } catch (e) {
      console.warn("OCR failed:", uri, e?.message || e);
    }
  }
  return full;
}

// ===== Customer lookup (Firestore master) =====
async function detectCustomerFromMaster(sourceText) {
  try {
    const snap = await customerDb.collection("jsons").doc("Client Search").get();
    if (!snap.exists) return null;

    const doc = snap.data();
    let sheet = doc.main;
    if (typeof sheet === "string") sheet = JSON.parse(sheet);

    const matrix = sheet?.tables?.[0]?.matrix;
    if (!Array.isArray(matrix) || matrix.length < 2) return null;

    const header = matrix[0];
    const idx = (colName) => header.indexOf(colName);

    const colId = idx("id");
    const colName = idx("name");
    const colMailAliases = idx("mail_aliases");
    const colFaxAliases = idx("fax_aliases");
    const colNameAliases = idx("name_aliases");

    if (colId === -1 || colName === -1) return null;

    const normalize = (str) => String(str || "").toLowerCase().replace(/\s+/g, "");
    const normalizeDigits = (str) => String(str || "").replace(/[^\d]/g, "");

    const textNorm = normalize(sourceText);
    const textDigits = normalizeDigits(sourceText);

    const split = (v) =>
      String(v || "")
        .split(/[,\s、;／]+/)
        .map((x) => x.trim())
        .filter(Boolean);

    const rows = matrix.slice(1).map((row) => ({
      id: row[colId],
      name: row[colName],
      mailAliases: colMailAliases !== -1 ? split(row[colMailAliases]) : [],
      faxAliases: colFaxAliases !== -1 ? split(row[colFaxAliases]) : [],
      nameAliases: colNameAliases !== -1 ? split(row[colNameAliases]) : [],
    }));

    for (const r of rows) for (const a of r.mailAliases) if (normalize(a) && textNorm.includes(normalize(a))) return { id: r.id, name: r.name };
    for (const r of rows) for (const a of r.faxAliases) if (normalizeDigits(a) && textDigits.includes(normalizeDigits(a))) return { id: r.id, name: r.name };
    for (const r of rows) for (const a of r.nameAliases) if (normalize(a) && textNorm.includes(normalize(a))) return { id: r.id, name: r.name };
  } catch (e) {
    console.error("detectCustomerFromMaster error:", e);
  }
  return null;
}

// ===== Routes =====
app.get("/", (_req, res) => res.status(200).send("ok"));

registerProcessBatchRoutes(app, {
  supabase,
  runOcrForAttachments,
  detectCustomerFromMaster,
  bucket, // mail html->pdf で使う
});

// ===== Start =====
const port = process.env.PORT || 8080;
app.listen(port, "0.0.0.0", () => console.log(`worker listening on ${port}`));
