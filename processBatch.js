// processBatch.js
import puppeteer from "puppeteer";
import os from "os";
import path from "path";
import fs from "fs/promises";

/**
 * pending を少量処理
 * - messages から未処理を取ってロック
 * - fax: message_main_pdf_files からPDF取得 → OCR → 顧客特定 → messages 更新 → 最後に AK0000001S があれば messages.case_id のみ付け替え
 * - mail: body_text/body_html(+ヘッダ)で顧客特定、必要なら HTML→PDF 生成して main に設定
 * - thumbnail: PDF/HTML を JPEG サムネにして message_main_pdf_files.thumbnail_path に保存
 */
export function registerProcessBatchRoutes(app, deps) {
  const { supabase, runOcrForAttachments, detectCustomerFromMaster, bucket } = deps;

  app.post("/gmail/process-batch", async (req, res) => {
    const started = Date.now();
    const limit = Math.min(Number(req.query?.limit || 2), 10);
    const lockMinutes = 10;

    try {
      const { data: rows, error: selErr } = await supabase
        .from("messages")
        .select(
          "id, case_id, message_type, subject, body_text, body_html, from_email, to_email, received_at, main_pdf_path"
        )
        .is("processed_at", null)
        .or(
          `processing_at.is.null,processing_at.lt.${new Date(
            Date.now() - lockMinutes * 60 * 1000
          ).toISOString()}`
        )
        .order("received_at", { ascending: false })
        .limit(limit);

      if (selErr) throw selErr;

      let processed = 0;

      for (const m of rows || []) {
        const nowIso = new Date().toISOString();

        // ロック
        const { error: lockErr } = await supabase
          .from("messages")
          .update({ processing_at: nowIso, ocr_status: "processing", ocr_error: null })
          .eq("id", m.id)
          .is("processed_at", null);

        if (lockErr) {
          console.error("lockErr:", lockErr);
          continue;
        }

        try {
          let attachments = [];
          let mainPdfPath = null;

          // サムネ更新対象（faxは既存 row を更新する）
          let mainPdfFileRowId = null;

          if (m.message_type === "fax") {
            const { data: pdfRows, error: pdfErr } = await supabase
              .from("message_main_pdf_files")
              .select("id, gcs_path")
              .eq("message_id", m.id)
              .order("created_at", { ascending: true })
              .limit(10);

            if (pdfErr) throw pdfErr;

            attachments = (pdfRows || []).map((r) => r.gcs_path).filter(Boolean);
            mainPdfPath = attachments[0] || null;
            mainPdfFileRowId = pdfRows?.[0]?.id ?? null;
          } else {
            const { data: attRows, error: attErr } = await supabase
              .from("message_attachments")
              .select("gcs_path")
              .eq("message_id", m.id);

            if (attErr) throw attErr;
            attachments = (attRows || []).map((r) => r.gcs_path).filter(Boolean);

            // mail: HTML→PDF を後処理で生成し、mainにする（既にあるならスキップ）
            if (!m.main_pdf_path) {
              const htmlSource =
                m.body_html ||
                (m.body_text ? `<pre>${escapeHtml(String(m.body_text))}</pre>` : null);

              if (htmlSource) {
                try {
                  // 1) HTML -> PDF
                  const renderedPdf = await renderMailHtmlToPdfToGcs({
                    bucket,
                    messageId: m.id,
                    html: htmlSource,
                  });

                  if (renderedPdf) {
                    mainPdfPath = renderedPdf;

                    // 2) HTML -> JPEG thumbnail
                    let thumbPath = null;
                    try {
                      thumbPath = await renderHtmlToJpegToGcs({
                        bucket,
                        messageId: m.id,
                        html: htmlSource,
                      });
                    } catch (e) {
                      console.error("html thumbnail failed:", m.id, e?.message || e);
                    }

                    // message_main_pdf_files に登録（mail_rendered）
                    const { error: insErr } = await supabase.from("message_main_pdf_files").insert({
                      case_id: m.case_id,
                      message_id: m.id,
                      gcs_path: mainPdfPath,
                      file_name: mainPdfPath.split("/").pop() || null,
                      mime_type: "application/pdf",
                      file_type: "mail_rendered",
                      thumbnail_path: thumbPath,
                    });

                    if (insErr) console.error("insert message_main_pdf_files failed:", insErr);
                  }
                } catch (e) {
                  console.error("mail html=>pdf failed:", m.id, e?.message || e);
                }
              }
            } else {
              // 既にmainがあるならそれを使う（レンダ再生成しない）
              mainPdfPath = m.main_pdf_path;
            }
          }

          let ocrText = "";
          let customer = null;

          if (m.message_type === "fax") {
            // fax: PDF OCR
            ocrText = await runOcrForAttachments(attachments);

            const head = String(ocrText || "").slice(0, 200);
            customer =
              (head && (await detectCustomerFromMaster(head))) ||
              (await detectCustomerFromMaster(ocrText));
          } else {
            // mail: body + subject + ヘッダも含めて顧客特定
            const source = `${m.subject || ""}\n${m.from_email || ""}\n${m.to_email || ""}\n${
              m.body_text || ""
            }\n${m.body_html || ""}`;

            const head = String(source || "").slice(0, 400);
            customer =
              (head && (await detectCustomerFromMaster(head))) ||
              (await detectCustomerFromMaster(source));
          }

          // fax: PDF -> JPEG サムネ（1ページ目）
          if (m.message_type === "fax" && mainPdfPath) {
            try {
              const thumbPath = await renderPdfFirstPageToJpegToGcs({
                bucket,
                gcsPdfPath: mainPdfPath,
                messageId: m.id,
              });

              // 既存 row（先頭）に thumbnail_path を入れる
              if (thumbPath && mainPdfFileRowId) {
                const { error: thErr } = await supabase
                  .from("message_main_pdf_files")
                  .update({ thumbnail_path: thumbPath })
                  .eq("id", mainPdfFileRowId);

                if (thErr) console.error("update thumbnail_path failed:", thErr);
              }
            } catch (e) {
              console.error("pdf thumbnail failed:", m.id, e?.message || e);
            }
          }

          // messages 更新
          // 注意: mail の body_text を ocrText(空)で上書きしない
          const updatePayload = {
            main_pdf_path: mainPdfPath ?? null,
            customer_id: customer?.id ?? 0,
            customer_name: customer?.name ?? "未設定",
            ocr_status: "done",
            processed_at: new Date().toISOString(),
            processing_at: null,
          };

          // fax の本文は body_text に保存したいならここで入れる（mailは上書きしない）
          if (m.message_type === "fax") {
            updatePayload.body_text = ocrText || "";
            updatePayload.body_type = "ocr_pdf";
            updatePayload.ocr_text = ocrText || "";
          }

          const { error: updErr } = await supabase
            .from("messages")
            .update(updatePayload)
            .eq("id", m.id);

          if (updErr) throw updErr;

          // ★最後に AK0000001S を OCR から抽出して messages.case_id のみ更新
          if (m.message_type === "fax") {
            const akDigits7 = extractCaseIdFromOcr(ocrText);

            if (akDigits7) {
              const akCaseId = Number(akDigits7); // "0000001" -> 1

              if (Number.isFinite(akCaseId) && akCaseId > 0) {
                const { data: caseRow, error: caseSelErr } = await supabase
                  .from("cases")
                  .select("id")
                  .eq("id", akCaseId)
                  .maybeSingle();

                if (caseSelErr) {
                  console.error("case lookup error:", caseSelErr);
                } else if (caseRow?.id) {
                  const { error: updCaseErr } = await supabase
                    .from("messages")
                    .update({ case_id: akCaseId })
                    .eq("id", m.id);

                  if (updCaseErr) {
                    console.error("update message.case_id by AK failed:", updCaseErr);
                  } else {
                    console.log(
                      "AK detected => relinked message.case_id only:",
                      akCaseId,
                      "msg:",
                      m.id
                    );
                  }
                }
              }
            }
          }

          processed++;
        } catch (e) {
          console.error("process one error:", m.id, e);

          await supabase
            .from("messages")
            .update({
              ocr_status: "error",
              ocr_error: e?.message || String(e),
              processing_at: null,
            })
            .eq("id", m.id);
        }
      }

      res
        .status(200)
        .send(
          `OK processed=${processed} picked=${(rows || []).length} in ${Date.now() - started}ms`
        );
    } catch (e) {
      console.error("/gmail/process-batch error:", e);
      res.status(500).send("error");
    }
  });
}

/* ================= util ================= */

function sanitizeId(id) {
  return String(id || "")
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .slice(0, 120);
}

function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function parseGsPath(gsPath) {
  const m = String(gsPath || "").match(/^gs:\/\/([^/]+)\/(.+)$/);
  if (!m) throw new Error(`invalid gs path: ${gsPath}`);
  return { bucketName: m[1], objectPath: m[2] };
}

/**
 * OCRテキストから「AK0000001S」系を拾って 7桁を返す
 * - 誤読(O/I/l) を補正
 * - 返り値は "0000001" のような 7桁文字列
 */
function extractCaseIdFromOcr(text) {
  if (!text) return null;

  const m = String(text).match(/AK\s*([0-9OIl]{7})\s*[S5]/i);
  if (!m) return null;

  let s = String(m[1] || "").toUpperCase();
  s = s.replace(/O/g, "0").replace(/[IL]/g, "1");

  if (!/^\d{7}$/.test(s)) return null;
  return s;
}

async function launchBrowser() {
  const executablePath = process.env.PUPPETEER_EXECUTABLE_PATH || undefined;

  return puppeteer.launch({
    executablePath,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
}

/* ================= HTML → PDF（メール用） ================= */

async function renderMailHtmlToPdfToGcs({ bucket, messageId, html }) {
  if (!bucket) throw new Error("GCS bucket is not configured");

  const safeId = sanitizeId(messageId);
  const objectPath = `mail_rendered/${safeId}.pdf`;

  const browser = await launchBrowser();
  try {
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: "networkidle0" });

    const pdfBuffer = await page.pdf({
      format: "A4",
      printBackground: true,
    });

    await bucket.file(objectPath).save(pdfBuffer, {
      resumable: false,
      metadata: { contentType: "application/pdf" },
    });

    return `gs://${bucket.name}/${objectPath}`;
  } finally {
    await browser.close().catch(() => {});
  }
}

/* ================= HTML → JPEG（サムネ） ================= */

async function renderHtmlToJpegToGcs({ bucket, messageId, html }) {
  if (!bucket) throw new Error("GCS bucket is not configured");

  const safeId = sanitizeId(messageId);
  const objectPath = `thumbnails/${safeId}.jpg`;

  const browser = await launchBrowser();
  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1200, height: 1600 });
    await page.setContent(html, { waitUntil: "networkidle0" });

    const jpegBuffer = await page.screenshot({
      type: "jpeg",
      quality: 80,
      fullPage: false,
    });

    await bucket.file(objectPath).save(jpegBuffer, {
      resumable: false,
      metadata: { contentType: "image/jpeg" },
    });

    return `gs://${bucket.name}/${objectPath}`;
  } finally {
    await browser.close().catch(() => {});
  }
}

/* ================= PDF → JPEG（1ページ目サムネ） ================= */

async function renderPdfFirstPageToJpegToGcs({ bucket, gcsPdfPath, messageId }) {
  if (!bucket) throw new Error("GCS bucket is not configured");
  if (!gcsPdfPath) throw new Error("gcsPdfPath is required");

  const { bucketName, objectPath: srcObjectPath } = parseGsPath(gcsPdfPath);

  const safeId = sanitizeId(messageId);
  const dstObjectPath = `thumbnails/${safeId}.jpg`;

  const tmpPdf = path.join(os.tmpdir(), `${safeId}.pdf`);

  const srcBucket =
    bucket.name === bucketName ? bucket : bucket.storage.bucket(bucketName);

  await srcBucket.file(srcObjectPath).download({ destination: tmpPdf });

  const browser = await launchBrowser();
  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1200, height: 1600 });

    await page.goto(`file://${tmpPdf}`, { waitUntil: "networkidle0" });

    const jpegBuffer = await page.screenshot({
      type: "jpeg",
      quality: 80,
      fullPage: false,
    });

    await bucket.file(dstObjectPath).save(jpegBuffer, {
      resumable: false,
      metadata: { contentType: "image/jpeg" },
    });

    return `gs://${bucket.name}/${dstObjectPath}`;
  } finally {
    await browser.close().catch(() => {});
    await fs.unlink(tmpPdf).catch(() => {});
  }
}
