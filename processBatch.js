// processBatch.js
import puppeteer from "puppeteer";

/**
 * 2) pending を少量処理
 * - messages から未処理を取ってロック
 * - fax: message_main_pdf_files からPDF取得 → OCR → 顧客特定 → cases/messages 更新
 * - mail: body_text/body_html(+必要ならヘッダ)で顧客特定、さらに HTML→PDF を後処理で生成
 */
export function registerProcessBatchRoutes(app, deps) {
  const {
    supabase,
    runOcrForAttachments,
    detectCustomerFromMaster,
    bucket, // ★ index.js からdepsで渡ってくる（既に渡している前提）
  } = deps;

  app.post("/gmail/process-batch", async (req, res) => {
    const started = Date.now();
    const limit = Math.min(Number(req.query?.limit || 2), 10);
    const lockMinutes = 10;

    try {
      const { data: rows, error: selErr } = await supabase
        .from("messages")
        .select("id, case_id, message_type, subject, body_text, body_html, from_email, to_email, received_at, main_pdf_path")
        .is("processed_at", null)
        .or(`processing_at.is.null,processing_at.lt.${new Date(Date.now() - lockMinutes * 60 * 1000).toISOString()}`)
        .order("received_at", { ascending: false })
        .limit(limit);

      if (selErr) throw selErr;

      let processed = 0;

      for (const m of rows || []) {
        const nowIso = new Date().toISOString();

        // ロック（※現状の実装思想を保ちつつ。より厳密にするなら条件を追加推奨）
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

          if (m.message_type === "fax") {
            const { data: pdfRows, error: pdfErr } = 
              .from("message_main_pdf_files")
              .select("gcs_path")
              .eq("message_id", m.id)
              .order("created_at", { ascending: true })
              .limit(10);

            if (pdfErr) throw pdfErr;
            attachments = (pdfRows || []).map((r) => r.gcs_path).filter(Boolean);
            mainPdfPath = attachments[0] || null;
          } else {
            const { data: attRows, error: attErr } = 
              .from("message_attachments")
              .select("gcs_path")
              .eq("message_id", m.id);

            if (attErr) throw attErr;
            attachments = (attRows || []).map((r) => r.gcs_path).filter(Boolean);

            // ★mail: HTML→PDF を後処理で生成し、mainにする（既にあるならスキップ）
            if (!m.main_pdf_path) {
              const htmlSource =
                m.body_html ||
                (m.body_text ? `<pre>${escapeHtml(String(m.body_text))}</pre>` : null);

              if (htmlSource) {
                try {
                  const rendered = await renderMailHtmlToPdfToGcs({
                    bucket,
                    messageId: m.id,
                    html: htmlSource,
                  });

                  if (rendered) {
                    mainPdfPath = rendered;

                    // message_main_pdf_files に登録（mail_rendered）
                    .from("message_main_pdf_files").insert({
                      case_id: m.case_id,
                      message_id: m.id,
                      gcs_path: mainPdfPath,
                      file_name: mainPdfPath.split("/").pop() || null,
                      mime_type: "application/pdf",
                      file_type: "mail_rendered",
                      thumbnail_path: null,
                    });
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
            ocrText = await runOcrForAttachments(attachments);
            const head = String(ocrText || "").slice(0, 200);
            customer = (head && (await detectCustomerFromMaster(head))) || (await detectCustomerFromMaster(ocrText));
          } else {
            // ★mail: body + subject + ヘッダも含めて顧客特定（アドレス確定が効く）
            const source =
              `${m.subject || ""}\n${m.from_email || ""}\n${m.to_email || ""}\n${m.body_text || ""}\n${m.body_html || ""}`;

            const head = String(source || "").slice(0, 400);
            customer =
              (head && (await detectCustomerFromMaster(head))) ||
              (await detectCustomerFromMaster(source));
          }

          // cases 更新（顧客が取れた場合）
          if (m.case_id && (customer?.id || customer?.name)) {
            await supabase
              .from("cases")
              .update({
                customer_id: customer?.id ?? 0,
                customer_name: customer?.name ??  "未設定",
                latest_message_at: m.received_at ?? null,
                title: m.subject ?? null,
              })
              .eq("id", m.case_id);
          }

          const { error: updErr } = await supabase
            .from("messages")
            .update({
              main_pdf_path: mainPdfPath ?? null,
              body_text: ocrText,
              customer_id: customer?.id ?? 0,
              customer_name: customer?.name ??  "未設定",
              ocr_status: "done",
              processed_at: new Date().toISOString(),
              processing_at: null,
            })
            .eq("id", m.id);

          if (updErr) throw updErr;

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

      res.status(200).send(`OK processed=${processed} picked=${(rows || []).length} in ${Date.now() - started}ms`);
    } catch (e) {
      console.error("/gmail/process-batch error:", e);
      res.status(500).send("error");
    }
  });
}

/* ================= HTML → PDF（メール用） ================= */

function sanitizeId(id) {
  return String(id || "").replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 120);
}

function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function renderMailHtmlToPdfToGcs({ bucket, messageId, html }) {
  if (!bucket) throw new Error("GCS bucket is not configured");

  const safeId = sanitizeId(messageId);
  const objectPath = `mail_rendered/${safeId}.pdf`;

  const browser = await puppeteer.launch({
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

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
