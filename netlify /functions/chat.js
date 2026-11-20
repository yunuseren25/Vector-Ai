async function processGroqChat(method, rawBody, apiKey) {
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
      body: JSON.stringify({ error: "GROQ_API_KEY is missing in Netlify Environment Variables." })
    };
  }

  let data = {};
  try {
    data = typeof rawBody === "string" ? JSON.parse(rawBody || "{}") : (rawBody || {});
  } catch {
    return {
      status: 400,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Invalid JSON request body." })
    };
  }

  const { messages = [], systemInstruction = "", mode = "general" } = data;

  const formattedMessages = [];
  if (systemInstruction && typeof systemInstruction === "string" && systemInstruction.trim()) {
    formattedMessages.push({
      role: "system",
      content: systemInstruction.trim()
    });
  }

  const cleanHistory = messages
    .filter((m) => m && typeof m.content === "string" && m.content.trim().length > 0)
    .slice(-16)
    .map((m) => ({
      role: m.role === "assistant" ? "assistant" : "user",
      content: m.content.trim()
    }));

  formattedMessages.push(...cleanHistory);

  if (cleanHistory.length === 0) {
    return {
      status: 400,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "No valid message content provided." })
    };
  }

  const upstream = await fetch("https://api.groq.com/openai/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model: "llama-3.3-70b-versatile",
      messages: formattedMessages,
      temperature: mode === "code" ? 0.25 : 0.65,
      max_tokens: 2048,
      stream: true
    })
  });

  if (!upstream.ok) {
    const errText = await upstream.text();
    return {
      status: upstream.status,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        error: `Groq API error (${upstream.status})`,
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
            if (!raw) continue;
            if (raw === "[DONE]") {
              controller.enqueue(encoder.encode("data: [DONE]\n\n"));
              continue;
            }

            try {
              const parsed = JSON.parse(raw);
              const deltaText = parsed.choices?.[0]?.delta?.content;
              if (deltaText) {
                controller.enqueue(
                  encoder.encode(`data: ${JSON.stringify({ text: deltaText })}\n\n`)
                );
              }
            } catch {}
          }
        }
        controller.enqueue(encoder.encode("data: [DONE]\n\n"));
      } catch (err) {
        controller.enqueue(
          encoder.encode(`data: ${JSON.stringify({ error: err.message || "Stream failed" })}\n\n`)
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

export default async function handler(req) {
  const apiKey = process.env.GROQ_API_KEY;

  if (req && typeof req.text === "function") {
    const rawBody = await req.text();
    const res = await processGroqChat(req.method, rawBody, apiKey);
    if (res.stream) {
      return new Response(res.stream, { status: res.status, headers: res.headers });
    }
    return new Response(res.body, { status: res.status, headers: res.headers });
  }

  const res = await processGroqChat(req.httpMethod, req.body, apiKey);
  if (res.stream) {
    const reader = res.stream.getReader();
    const decoder = new TextDecoder();
    let out = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      out += decoder.decode(value, { stream: true });
    }
    return { statusCode: res.status, headers: res.headers, body: out };
  }

  return { statusCode: res.status, headers: res.headers, body: res.body };
}
