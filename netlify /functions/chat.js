async function handleChatRequest(method, rawBody, apiKey) {
  if (method !== "POST") {
    return {
      status: 405,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Method Not Allowed" })
    };
  }

  if (!apiKey) {
    return {
      status: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "GEMINI_API_KEY is missing in Netlify Environment Variables." })
    };
  }

  let parsedBody = {};
  try {
    parsedBody = typeof rawBody === "string" ? JSON.parse(rawBody || "{}") : (rawBody || {});
  } catch {
    return {
      status: 400,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Invalid JSON payload." })
    };
  }

  const { messages = [], systemInstruction = "", mode = "general" } = parsedBody;

  const contents = messages
    .filter((m) => m && typeof m.content === "string" && m.content.trim().length > 0)
    .slice(-16)
    .map((m) => ({
      role: m.role === "assistant" ? "model" : "user",
      parts: [{ text: m.content.trim() }]
    }));

  if (contents.length === 0) {
    return {
      status: 400,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Please provide at least one message." })
    };
  }

  const model = "gemini-2.5-flash";
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:streamGenerateContent?alt=sse&key=${encodeURIComponent(apiKey)}`;

  const payload = {
    contents,
    generationConfig: {
      temperature: mode === "code" ? 0.3 : 0.65,
      maxOutputTokens: 2048,
      thinkingConfig: {
        thinkingBudget: mode === "code" ? 512 : 0
      }
    }
  };

  if (systemInstruction && typeof systemInstruction === "string") {
    payload.systemInstruction = {
      parts: [{ text: systemInstruction }]
    };
  }

  const upstream = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });

  if (!upstream.ok) {
    const errText = await upstream.text();
    return {
      status: upstream.status,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        error: `Gemini API error (${upstream.status})`,
        details: errText.slice(0, 240)
      })
    };
  }

  const encoder = new TextEncoder();
  const decoder = new TextDecoder("utf-8");

  const stream = new ReadableStream({
    async start(controller) {
      const reader = upstream.body.getReader();
      let buffer = "";

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";

          for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed.startsWith("data:")) continue;
            const raw = trimmed.slice(5).trim();
            if (!raw || raw === "[DONE]") continue;

            try {
              const data = JSON.parse(raw);
              const parts = data.candidates?.[0]?.content?.parts || [];
              for (const part of parts) {
                if (part.text) {
                  controller.enqueue(
                    encoder.encode(`data: ${JSON.stringify({ text: part.text })}\n\n`)
                  );
                }
              }
            } catch {}
          }
        }
        controller.enqueue(encoder.encode("data: [DONE]\n\n"));
      } catch (err) {
        controller.enqueue(
          encoder.encode(`data: ${JSON.stringify({ error: err.message || "Stream error" })}\n\n`)
        );
      } finally {
        controller.close();
      }
    }
  });

  return {
    status: 200,
    headers: {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      "Connection": "keep-alive"
    },
    stream
  };
}

export default async function handler(req, context) {
  const apiKey = process.env.GEMINI_API_KEY;
  if (req && typeof req.text === "function") {
    const rawBody = await req.text();
    const res = await handleChatRequest(req.method, rawBody, apiKey);
    if (res.stream) {
      return new Response(res.stream, { status: res.status, headers: res.headers });
    }
    return new Response(res.body, { status: res.status, headers: res.headers });
  }

  const res = await handleChatRequest(req.httpMethod, req.body, apiKey);
  if (res.stream) {
    const reader = res.stream.getReader();
    const decoder = new TextDecoder();
    let full = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      full += decoder.decode(value, { stream: true });
    }
    return { statusCode: res.status, headers: res.headers, body: full };
  }

  return { statusCode: res.status, headers: res.headers, body: res.body };
}
