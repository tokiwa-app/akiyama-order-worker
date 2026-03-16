// processBatch.js

/**
 * pending を少量処理
 * - messages から未処理を取ってロック
 * - fax: PDF OCR → 顧客特定 → messages 更新 → AK0000001S があれば messages.case_id 更新
 * - mail: 件名 → 本文 の順で AK0000001S 検出 + 顧客特定
 */
export function registerProcessBatchRoutes(app, deps) {
  const { supabase, runOcrForAttachments, detectCustomerFromMaster } = deps;

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
          .update({
            processing_at: nowIso,
            ocr_status: "processing",
            ocr_error: null,
          })
          .eq("id", m.id)
          .is("processed_at", null);

        if (lockErr) {
          console.error("lockErr:", lockErr);
          continue;
        }

        try {
          let attachments = [];
          let mainPdfPath = m.main_pdf_path || null;
          let ocrText = "";
          let customer = null;

          // =========================
          // FAX
          // =========================
          if (m.message_type === "fax") {
            const { data: pdfRows, error: pdfErr } = await supabase
              .from("message_main_pdf_files")
              .select("gcs_path")
              .eq("message_id", m.id)
              .order("created_at", { ascending: true })
              .limit(10);

            if (pdfErr) throw pdfErr;

            attachments = (pdfRows || [])
              .map((r) => r.gcs_path)
              .filter(Boolean);

            mainPdfPath = attachments[0] || null;

            // OCR
            ocrText = await runOcrForAttachments(attachments);

            const head = String(ocrText || "").slice(0, 200);

            customer =
              (head && (await detectCustomerFromMaster(head))) ||
              (await detectCustomerFromMaster(ocrText));
          }

          // =========================
          // MAIL
          // =========================
          else {
            const source = `${m.subject || ""}\n${m.from_email || ""}\n${m.to_email || ""}\n${
              m.body_text || ""
            }\n${m.body_html || ""}`;

            const head = String(source || "").slice(0, 400);

            customer =
              (head && (await detectCustomerFromMaster(head))) ||
              (await detectCustomerFromMaster(source));
          }

          // =========================
          // cases 更新
          // =========================
          if (m.case_id) {
            await supabase
              .from("cases")
              .update({
                customer_id: customer?.id ?? 0,
                customer_name: customer?.name ?? "未設定",
                latest_message_at: m.received_at ?? null,
                title: m.subject ?? null,
              })
              .eq("id", m.case_id);
          }

          // =========================
          // messages 更新
          // =========================
          const updatePayload = {
            main_pdf_path: mainPdfPath ?? null,
            customer_id: customer?.id ?? 0,
            customer_name: customer?.name ?? "未設定",
            ocr_status: "done",
            processed_at: new Date().toISOString(),
            processing_at: null,
          };

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

          // =========================
          // AK0000001S 検出
          // =========================
          const akDigits7 = extractCaseIdForMessage(m, ocrText);

          if (akDigits7) {
            const akCaseId = Number(akDigits7);

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
                  console.error(
                    "update message.case_id by AK failed:",
                    updCaseErr
                  );
                } else {
                  console.log(
                    "AK detected => relinked message.case_id:",
                    akCaseId,
                    "msg:",
                    m.id
                  );
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
          `OK processed=${processed} picked=${(rows || []).length} in ${
            Date.now() - started
          }ms`
        );
    } catch (e) {
      console.error("/gmail/process-batch error:", e);
      res.status(500).send("error");
    }
  });
}

/**
 * mail / fax 共通で AK番号抽出
 * 優先順位
 * 1. 件名
 * 2. 本文
 * 3. OCR
 */
function extractCaseIdForMessage(m, ocrText) {
  if (m.message_type === "fax") {
    return extractCaseIdFromText(ocrText || "");
  }

  const fromSubject = extractCaseIdFromText(m.subject || "");
  if (fromSubject) return fromSubject;

  return extractCaseIdFromText(
    `${m.body_text || ""}\n${m.body_html || ""}`
  );
}

/**
 * AK0000001S 形式抽出
 */
function extractCaseIdFromText(text) {
  if (!text) return null;

  const m = String(text).match(/AK\s*([0-9OIl]{7})\s*[S5]/i);
  if (!m) return null;

  let s = String(m[1] || "").toUpperCase();

  s = s.replace(/O/g, "0").replace(/[IL]/g, "1");

  if (!/^\d{7}$/.test(s)) return null;

  return s;
}
